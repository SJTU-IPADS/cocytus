/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  The code is part of the Cocytus project
 *
 * Copyright (C) 2016 Institute of Parallel and Distributed Systems (IPADS), Shanghai Jiao Tong University
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more about this software, visit:  http://ipads.se.sjtu.edu.cn/pub/projects/cocytus
 *
 */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include "ecalloc.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;


typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */


#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) (hashsize(n)-1)


struct hashtable {
    /* Main hash table. This is where we look except during expansion. */
    item **dict;
    /*
     * Previous hash table. During expansion, we look here for keys that haven't
     * been moved over to the primary yet.
     */
    item **old_dict;

    /* Flag: Are we in the middle of expanding now? */
    bool expanding;
    bool started_expanding;

    /* Number of items in the hash table. */
    unsigned int hash_items;

    /* how many powers of 2's worth of buckets we use */
    //unsigned int hashpower = HASHPOWER_DEFAULT;
    unsigned int hashpower;
    /*
     * During expansion we migrate values with bucket granularity; this is how
     * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
     */
    unsigned int expand_bucket;
};

struct hashtable *hashtables;
__thread struct hashtable *hashtable;

extern struct ecalloc *ecalloc;
extern struct ecalloc *ecallocs;

#ifdef ENABLE_COCYTUS
void
switch_hashtable(const int lid) {
    hashtable = &hashtables[lid];
    ecalloc = &ecallocs[lid];
}
#endif

void assoc_init(const int hashtable_init) {
    unsigned int hashpower = 0;
    if (hashtable_init) {
        hashpower = hashtable_init;
    } else {
        hashpower = HASHPOWER_DEFAULT;
    }
#ifdef ENABLE_COCYTUS
    int i;
    if (IS_PARITY_SERVER) {
        hashtables = calloc(settings.nshard, sizeof(struct hashtable));
        for (i=0; i<settings.nshard; ++i) {
            hashtables[i].dict = calloc(hashsize(hashpower), sizeof(void *));
            if (!hashtables[i].dict) {
                fprintf(stderr, "Failed to init hashtable.\n");
                exit(EXIT_FAILURE);
            }
            hashtables[i].expanding = false;
            hashtables[i].started_expanding = false;
            hashtables[i].hash_items = 0;
            hashtables[i].hashpower = hashpower;
            hashtables[i].expand_bucket = 0;
        }
        hashtable = NULL;
    } else
#endif
    {
        hashtables = calloc(1, sizeof(struct hashtable));
        hashtable = &hashtables[0];
        hashtable->dict = calloc(hashsize(hashpower), sizeof(void *));
        if (!hashtable->dict) {
            fprintf(stderr, "Failed to init hashtable->\n");
            exit(EXIT_FAILURE);
        }
        hashtable->expanding = false;
        hashtable->started_expanding = false;
        hashtable->hash_items = 0;
        hashtable->hashpower = hashpower;
        hashtable->expand_bucket = 0;
    }

    STATS_LOCK();
    stats.hash_power_level = hashpower;
    stats.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

#ifdef ENABLE_COCYTUS
#define HASHTABLE_CHECK do {                \
    if (!hashtable) {   \
        printf("hashtable check error\n");                  \
        exit(-1);                           \
    }                                       \
} while (0)
#else
#define HASHTABLE_CHECK do{}while(0)
#endif

item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    unsigned int oldbucket;

    HASHTABLE_CHECK;

    if (hashtable->expanding &&
        (oldbucket = (hv & hashmask(hashtable->hashpower - 1))) >= hashtable->expand_bucket)
    {
        it = hashtable->old_dict[oldbucket];
    } else {
        it = hashtable->dict[hv & hashmask(hashtable->hashpower)];
    }

    item *ret = NULL;
    int depth = 0;
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    unsigned int oldbucket;

    HASHTABLE_CHECK;

    if (hashtable->expanding &&
        (oldbucket = (hv & hashmask(hashtable->hashpower - 1))) >= hashtable->expand_bucket)
    {
        pos = &hashtable->old_dict[oldbucket];
    } else {
        pos = &hashtable->dict[hv & hashmask(hashtable->hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static void assoc_expand(void) {

    HASHTABLE_CHECK;

    hashtable->old_dict = hashtable->dict;

    hashtable->dict = calloc(hashsize(hashtable->hashpower + 1), sizeof(void *));
    if (hashtable->dict) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashtable->hashpower++;
        hashtable->expanding = true;
        hashtable->expand_bucket = 0;
        // TODO: update the stats
        STATS_LOCK();
        stats.hash_power_level = hashtable->hashpower;
        stats.hash_bytes += hashsize(hashtable->hashpower) * sizeof(void *);
        stats.hash_is_expanding = 1;
        STATS_UNLOCK();
    } else {
        hashtable->dict = hashtable->old_dict;
        /* Bad news, but we can keep running. */
    }
}

static void assoc_start_expand(void) {

    HASHTABLE_CHECK;

    if (hashtable->started_expanding)
        return;

    hashtable->started_expanding = true;
    pthread_cond_signal(&maintenance_cond);
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it, const uint32_t hv) {
    unsigned int oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */
    HASHTABLE_CHECK;

    if (hashtable->expanding &&
        (oldbucket = (hv & hashmask(hashtable->hashpower - 1))) >= hashtable->expand_bucket)
    {
        it->h_next = hashtable->old_dict[oldbucket];
        hashtable->old_dict[oldbucket] = it;
    } else {
        it->h_next = hashtable->dict[hv & hashmask(hashtable->hashpower)];
        hashtable->dict[hv & hashmask(hashtable->hashpower)] = it;
    }

    hashtable->hash_items++;
    if (! hashtable->expanding && hashtable->hash_items > (hashsize(hashtable->hashpower) * 3) / 2) {
        assoc_start_expand();
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hashtable->hash_items);
    return 1;
}

void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv);
    HASHTABLE_CHECK;

    if (*before) {
        item *nxt;
        hashtable->hash_items--;
        /* The DTrace probe cannot be triggered as the last instruction
         * due to possible tail-optimization by the compiler
         */
        MEMCACHED_ASSOC_DELETE(key, nkey, hashtable->hash_items);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = nxt;
        return;
    }
    /* Note:  we never actually get here.  the callers don't delete things
       they can't find. */
    assert(*before != 0);
}


static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void *arg) {

    //HASHTABLE_CHECK;
#ifdef ENABLE_COCYTUS
    int n = 1;
    if (IS_PARITY_SERVER) {
        n = settings.nshard;
    }
#endif

    while (do_run_maintenance_thread) {
#ifdef ENABLE_COCYTUS
        int i;
        for (i=0; i<n; ++i) {
            hashtable = &hashtables[i];
#endif

        int ii = 0;

        /* Lock the cache, and bulk move multiple buckets to the new
         * hash table. */
        item_lock_global();
        mutex_lock(&cache_lock);

        for (ii = 0; ii < hash_bulk_move && hashtable->expanding; ++ii) {
            item *it, *next;
            int bucket;

            for (it = hashtable->old_dict[hashtable->expand_bucket]; NULL != it; it = next) {
                next = it->h_next;

                bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashtable->hashpower);
                it->h_next = hashtable->dict[bucket];
                hashtable->dict[bucket] = it;
            }

            hashtable->old_dict[hashtable->expand_bucket] = NULL;

            hashtable->expand_bucket++;
            if (hashtable->expand_bucket == hashsize(hashtable->hashpower - 1)) {
                hashtable->expanding = false;
                free(hashtable->old_dict);
                // TODO: update stats
                STATS_LOCK();
                stats.hash_bytes -= hashsize(hashtable->hashpower - 1) * sizeof(void *);
                stats.hash_is_expanding = 0;
                STATS_UNLOCK();
                if (settings.verbose > 1)
                    fprintf(stderr, "Hash table expansion done\n");
            }
        }

        mutex_unlock(&cache_lock);
        item_unlock_global();
#ifdef ENABLE_COCYTUS
        }
        int expanding = 0;
        for (i=0; i<n; ++i) {
            if (hashtables[i].expanding) expanding++;
        }
        if (!expanding) {
#else
        if (!hashtable->expanding) {
#endif
            /* finished expanding. tell all threads to use fine-grained locks */
            switch_item_lock_type(ITEM_LOCK_GRANULAR);
            slabs_rebalancer_resume();
            /* We are done expanding.. just wait for next invocation */
            mutex_lock(&cache_lock);
            hashtable->started_expanding = false;
            pthread_cond_wait(&maintenance_cond, &cache_lock);
            /* Before doing anything, tell threads to use a global lock */
            mutex_unlock(&cache_lock);
            slabs_rebalancer_pause();
            switch_item_lock_type(ITEM_LOCK_GLOBAL);
            mutex_lock(&cache_lock);
            assoc_expand();
            mutex_unlock(&cache_lock);
        }
    }
    return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_assoc_maintenance_thread() {
    mutex_lock(&cache_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


