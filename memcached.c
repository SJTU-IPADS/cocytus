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
 *  memcached - memory caching daemon
 *
 *       http://www.memcached.org/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>

/* some POSIX systems need the following definition
 * to get mlockall flags out of sys/mman.h.  */
#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif
#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>

/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__) || defined(__APPLE__)
# define IOV_MAX 1024
#endif
#endif

#ifdef ENABLE_COCYTUS
#include "ecmem.h"
#include <jerasure.h>
#include <reed_sol.h>
#include <string.h>
#include <galois.h>
#include "rep_queue.h"
#include "queue.h"
#include "recovery.h"
#include <sys/time.h>
#include "bop_queue.h"
#include "pac_queue.h"
#include "const.h"
#endif


int *matrix;
/*
 * forward declarations
 */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);
static int try_read_command(conn *c);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

static void conn_set_state(conn *c, enum conn_states state);

/* token-related */
typedef struct token_s {
    char *value;
    size_t length;
} token_t;

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN 1

#define MAX_TOKENS 16


#ifdef ENABLE_COCYTUS

struct queue leader_ring;
struct ecmem ecmem;
struct ecalloc *ecallocs;
struct ecalloc *ecalloc;
struct recovery recovery;
struct rep_queue *rep_queue;
static int parse_line(FILE *f, char **line, size_t *size);

void parse_config_file(char *conf);

static void cocytus_init(void);

//static conn **peers_recv;
//static conn **peers_send;
struct peer *peers;

pthread_barrier_t thread_barrier;
static unsigned int peers_write_ready;
static unsigned int peers_read_ready;
//static pthread_mutex_t peers_ready_mutex;

// current leader's lid
static int recovery_leader;

// current process subs for subbed_lid
static int sub_as_lid;
static int sub_as_ready;
struct ecmem *sub_ecmem;
char *sub_flags; // 0 for not touched; 1 for recovering; 2 for recovered

static uint64_t sub_init_need_recovery_count = 0;
static uint64_t sub_finished_recovery_count = 0;
static int end_unit = 0;

static uint64_t alloc_xid;
static uint64_t stable_xid;
static uint64_t updated_xid;
static uint64_t stable_nxid;

static uint64_t total_w_req_bt_r = 0;
static uint64_t total_w_rep_bt_r = 0;
static uint64_t total_r_rep_bt_w = 0;
static uint64_t total_r_req_bt_w = 0;
static uint64_t w_req_cnt = 0;
static uint64_t w_rep_cnt = 0;
static uint64_t r_req_cnt = 0;
static uint64_t r_rep_cnt = 0;

//static pthread_mutex_t xid_mutex;

//static conn *idle_recovery_conn = NULL;
static struct event *idle_recovery_event;
static int recovery_start_unit = 0;
static int recovery_finished_unit = 0;
void idle_event_handler(const int fd, const short which, void *arg);
static bool update_idle_event(struct event* e, const int new_flags);

#ifndef DEBUG
long long mytime(int a);
#endif
long long mytime(int a)
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return (t.tv_sec * 1000000 + t.tv_usec) % 1000000000;
}

#endif

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c);
static void process_stat_settings(ADD_STAT add_stats, void *c);
static void conn_to_str(const conn *c, char *buf);


/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);
static void process_command(conn *c, char *command);
static void write_and_free(conn *c, char *buf, int bytes);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);
static void write_bin_error(conn *c, protocol_binary_response_status err,
                            const char *errstr, int swallow);

static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct settings settings;
time_t process_started;     /* when the process was started */
conn **conns;

struct slab_rebalance slab_rebal;
volatile int slab_rebalance_signal;

/** file scope variables **/
static conn *listen_conn = NULL;
static int max_fds;
static struct event_base *main_base;

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);

/* This reduces the latency without adding lots of extra wiring to be able to
 * notify the listener thread of when to listen again.
 * Also, the clock timer could be broken out into its own thread and we
 * can block the listener via a condition.
 */
static volatile bool allow_new_conns = true;
static struct event maxconnsevent;
static void maxconns_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 0, .tv_usec = 10000};

    if (fd == -42 || allow_new_conns == false) {
        /* reschedule in 10ms if we need to keep polling */
        evtimer_set(&maxconnsevent, maxconns_handler, 0);
        event_base_set(main_base, &maxconnsevent);
        evtimer_add(&maxconnsevent, &t);
    } else {
        evtimer_del(&maxconnsevent);
        accept_new_conns(true);
    }
}

#define REALTIME_MAXDELTA 60*60*24*30

/*
 * given time value that's either unix time or delta from current unix time, return
 * unix time. Use the fact that delta can't exceed one month (and real time value can't
 * be that low).
 */
static rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + current_time);
    }
}

#ifdef ENABLE_COCYTUS
// COCYTUS FUNCION DECLARATIONS
//

static int
need_recovery(int ubegin, int uend);

static void
sub_for(int subee);

void
restart_failed_recovery(int failed_lid);

static void
start_fast_recovery(int ubegin, int uend, int req_lid, conn *c);

static void
start_recovery(int ubegin, int uend, int req_lid, conn *c);

static void
assert_data_availability(uint64_t addr, int size);

static void
do_recovery(conn *c, int ubegin, int uend);

static int
try_do_recovery(conn *c, int ubegin, int uend, enum conn_states state_to_go);

void
process_queued_items(int deadlid, int maxxid);

void
shrink_recovery_interval(int *ubeign, int *uend);

int
add_iovs_from_recovery(conn *c, uint64_t addr, int size);

void
complete_recovery_bottom_half(struct recovery_queue_item *);

void
fill_completed_recovered_data(int ubegin, int uend, char *data);

static int cocytus_connect_to_peer(int i);

static int get_peer_lid_by_conn(conn *c);
static int get_real_peer_lid_by_conn(conn *c);

static int parity_send(conn *c, conn *target_c, uint64_t xid, item *it, void *diff);

static int conn_flush_wbuf(conn *c);

static int conn_touch_buf(conn *c, int s);

//static int send_short_msg(conn *c, char *s, char *e);

static int send_msgf(conn *c, const char *fmt, ...);

static inline int is_peer_conn(conn *c);

static int buf_set_u64(char *p, uint64_t n);

static int buf_set_u32(char *p, uint32_t n);

static void queue_rep_command(conn *c, token_t *tokens, const size_t ntoken,
        int comm, bool handle_cas);

static void process_rep_command(conn *c, uint64_t xid, int deadlid);

static int send_msgf_raw(conn *c, const char *fmt, ...);

static int send_msgbuf_raw(conn *c, char *buf, int nbuf);

static void send_recovered_data(int leader, int unit_begin, int unit_end, uint32_t mask);

int is_my_sharding(char *key, int nkey);
int is_my_sharding(char *key, int nkey)
{
    uint32_t hv;
    int gid, lid;
    hv = client_hash(key, nkey);
#ifdef DISABLE_GID
    gid = 0;
#endif
    gid = hv % settings.ngroup;
    lid = (hv / settings.ngroup) % settings.nshard;

    if (gid == settings.gid &&
            (lid == settings.lid || (lid == sub_as_lid && sub_as_ready))) {
        return 1;
    }
    printf("refusing request: hv=%"PRIu32" gid=%d lid=%d iam=%d nkey=%d key=%*s\n",
            hv, gid, lid, settings.lid, (int)nkey, (int)nkey, key);
    return 0;
}

extern LIBEVENT_THREAD *threads; /* in threads.c */

pthread_t worker_thread;
#endif





static void stats_init(void) {
    stats.curr_items = stats.total_items = stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
    stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = stats.evictions = stats.reclaimed = 0;
    stats.touch_cmds = stats.touch_misses = stats.touch_hits = stats.rejected_conns = 0;
    stats.malloc_fails = 0;
    stats.curr_bytes = stats.listen_disabled_num = 0;
    stats.hash_power_level = stats.hash_bytes = stats.hash_is_expanding = 0;
    stats.expired_unfetched = stats.evicted_unfetched = 0;
    stats.slabs_moved = 0;
    stats.accepting_conns = true; /* assuming we start in this state. */
    stats.slab_reassign_running = false;
    stats.lru_crawler_running = false;

    /* make the time we started always be 2 seconds before we really
       did, so time(0) - time.started is never zero.  if so, things
       like 'settings.oldest_live' which act as booleans as well as
       values are now false in boolean context... */
    process_started = time(0) - ITEM_UPDATE_INTERVAL - 2;
    stats_prefix_init();
}

static void stats_reset(void) {
    STATS_LOCK();
    stats.total_items = stats.total_conns = 0;
    stats.rejected_conns = 0;
    stats.malloc_fails = 0;
    stats.evictions = 0;
    stats.reclaimed = 0;
    stats.listen_disabled_num = 0;
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset();
    item_stats_reset();
}

static void settings_init(void) {
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 4;         /* N workers */
    settings.num_threads_per_udp = 0;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.maxconns_fast = false;
    settings.lru_crawler = false;
    settings.lru_crawler_sleep = 100;
    settings.lru_crawler_tocrawl = 0;
    settings.hashpower_init = 0;
    settings.slab_reassign = false;
    settings.slab_automove = 0;
    settings.shutdown_command = false;
    settings.tail_repair_time = TAIL_REPAIR_TIME_DEFAULT;
    settings.flush_enabled = true;
}

/*
 * Adds a message header to a connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int add_msghdr(conn *c)
{
    struct msghdr *msg;

    assert(c != NULL);

    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            return -1;
        }
        c->msglist = msg;
        c->msgsize *= 2;
    }

    msg = c->msglist + c->msgused;

    /* this wipes msg_iovlen, msg_control, msg_controllen, and
       msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));

    msg->msg_iov = &c->iov[c->iovused];

    if (IS_UDP(c->transport) && c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }

    c->msgbytes = 0;
    c->msgused++;

    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }

    return 0;
}

extern pthread_mutex_t conn_lock;

/*
 * Initializes the connections array. We don't actually allocate connection
 * structures until they're needed, so as to avoid wasting memory when the
 * maximum connection count is much higher than the actual number of
 * connections.
 *
 * This does end up wasting a few pointers' worth of memory for FDs that are
 * used for things other than connections, but that's worth it in exchange for
 * being able to directly index the conns array by FD.
 */
static void conn_init(void) {
    /* We're unlikely to see an FD much higher than maxconns. */
    int next_fd = dup(1);
    int headroom = 10;      /* account for extra unexpected open FDs */
    struct rlimit rl;

    max_fds = settings.maxconns + headroom + next_fd;

    /* But if possible, get the actual highest FD we can possibly ever see. */
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        max_fds = rl.rlim_max;
    } else {
        fprintf(stderr, "Failed to query maximum file descriptor; "
                        "falling back to maxconns\n");
    }

    close(next_fd);

    if ((conns = calloc(max_fds, sizeof(conn *))) == NULL) {
        fprintf(stderr, "Failed to allocate connection structures\n");
        /* This is unrecoverable so bail out early. */
        exit(1);
    }
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }
    return rv;
}

conn *conn_new(const int sfd, enum conn_states init_state,
                const int event_flags,
                const int read_buffer_size, enum network_transport transport,
                struct event_base *base) {
    conn *c;

    assert(sfd >= 0 && sfd < max_fds);
    c = conns[sfd];

    if (NULL == c) {
        if (!(c = (conn *)calloc(1, sizeof(conn)))) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate connection object\n");
            return NULL;
        }
        MEMCACHED_CONN_CREATE(c);

        c->rbuf = c->wbuf = 0;
        c->ilist = 0;
        c->suffixlist = 0;
        c->iov = 0;
        c->msglist = 0;
        c->hdrbuf = 0;

        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->isize = ITEM_LIST_INITIAL;
        c->suffixsize = SUFFIX_LIST_INITIAL;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;
        c->hdrsize = 0;

        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->ilist = (item **)malloc(sizeof(item *) * c->isize);
        c->suffixlist = (char **)malloc(sizeof(char *) * c->suffixsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);

        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 ||
                c->msglist == 0 || c->suffixlist == 0) {
            conn_free(c);
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            fprintf(stderr, "Failed to allocate buffers for connection\n");
            return NULL;
        }

        STATS_LOCK();
        stats.conn_structs++;
        STATS_UNLOCK();

        c->sfd = sfd;
        conns[sfd] = c;
    }

    c->transport = transport;
    c->protocol = settings.binding_protocol;

    /* unix socket mode doesn't need this, so zeroed out.  but why
     * is this done for every command?  presumably for UDP
     * mode.  */
    if (!settings.socketpath) {
        c->request_addr_size = sizeof(c->request_addr);
    } else {
        c->request_addr_size = 0;
    }

    if (transport == tcp_transport && init_state == conn_new_cmd) {
        if (getpeername(sfd, (struct sockaddr *) &c->request_addr,
                        &c->request_addr_size)) {
            perror("getpeername");
            memset(&c->request_addr, 0, sizeof(c->request_addr));
        }
    }

    if (settings.verbose > 1) {
        if (init_state == conn_listening) {
            fprintf(stderr, "<%d server listening (%s)\n", sfd,
                prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            fprintf(stderr, "<%d server listening (udp)\n", sfd);
        } else if (c->protocol == negotiating_prot) {
            fprintf(stderr, "<%d new auto-negotiating client connection\n",
                    sfd);
        } else if (c->protocol == ascii_prot) {
            fprintf(stderr, "<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            fprintf(stderr, "<%d new binary client connection.\n", sfd);
        } else {
            fprintf(stderr, "<%d new unknown (%d) client connection\n",
                sfd, c->protocol);
            assert(false);
        }
    }

    c->state = init_state;
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->authenticated = false;

    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;

    c->noreply = false;

    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;

    if (event_add(&c->event, 0) == -1) {
        perror("event_add");
        return NULL;
    }

    STATS_LOCK();
    stats.curr_conns++;
    stats.total_conns++;
    STATS_UNLOCK();

    MEMCACHED_CONN_ALLOCATE(c->sfd);

    return c;
}

static void conn_release_items(conn *c) {
    assert(c != NULL);

    if (c->item) {
        item_remove(c->item);
        c->item = 0;
    }

    while (c->ileft > 0) {
        item *it = *(c->icurr);
        assert((it->it_flags & ITEM_SLABBED) == 0);
        item_remove(it);
        c->icurr++;
        c->ileft--;
    }

    if (c->suffixleft != 0) {
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }
    }

    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    conn_release_items(c);

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }

    if (c->vbuf) {
        free(c->vbuf);
        c->vbuf = NULL;
        c->vnbytes = 0;
    }

    if (c->sasl_conn) {
        assert(settings.sasl);
        sasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

    if (IS_UDP(c->transport)) {
        conn_set_state(c, conn_read);
    }
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
    if (c) {
        assert(c != NULL);
        assert(c->sfd >= 0 && c->sfd < max_fds);

        MEMCACHED_CONN_DESTROY(c);
        conns[c->sfd] = NULL;
        if (c->hdrbuf)
            free(c->hdrbuf);
        if (c->msglist)
            free(c->msglist);
        if (c->rbuf)
            free(c->rbuf);
        if (c->wbuf)
            free(c->wbuf);
        if (c->ilist)
            free(c->ilist);
        if (c->suffixlist)
            free(c->suffixlist);
        if (c->iov)
            free(c->iov);
        free(c);
    }
}

static void conn_close(conn *c) {
    assert(c != NULL);

    /* delete the event, the socket and the conn */
    event_del(&c->event);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closed.\n", c->sfd);

    conn_cleanup(c);

    MEMCACHED_CONN_RELEASE(c->sfd);
    conn_set_state(c, conn_closed);
    close(c->sfd);

    pthread_mutex_lock(&conn_lock);
    allow_new_conns = true;
    pthread_mutex_unlock(&conn_lock);

    STATS_LOCK();
    stats.curr_conns--;
    STATS_UNLOCK();

    return;
}

/*
 * Shrinks a connection's buffers if they're too big.  This prevents
 * periodic large "get" requests from permanently chewing lots of server
 * memory.
 *
 * This should only be called in between requests since it can wipe output
 * buffers!
 */
static void conn_shrink(conn *c) {
    assert(c != NULL);

    if (IS_UDP(c->transport))
        return;

    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE) {
        char *newbuf;

        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t)c->rbytes);

        newbuf = (char *)realloc((void *)c->rbuf, DATA_BUFFER_SIZE);

        if (newbuf) {
            c->rbuf = newbuf;
            c->rsize = DATA_BUFFER_SIZE;
        }
        /* TODO check other branch... */
        c->rcurr = c->rbuf;
    }

    if (c->isize > ITEM_LIST_HIGHWAT) {
        item **newbuf = (item**) realloc((void *)c->ilist, ITEM_LIST_INITIAL * sizeof(c->ilist[0]));
        if (newbuf) {
            c->ilist = newbuf;
            c->isize = ITEM_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->msgsize > MSG_LIST_HIGHWAT) {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *)c->msglist, MSG_LIST_INITIAL * sizeof(c->msglist[0]));
        if (newbuf) {
            c->msglist = newbuf;
            c->msgsize = MSG_LIST_INITIAL;
        }
    /* TODO check error condition? */
    }

    if (c->iovsize > IOV_LIST_HIGHWAT) {
        struct iovec *newbuf = (struct iovec *) realloc((void *)c->iov, IOV_LIST_INITIAL * sizeof(c->iov[0]));
        if (newbuf) {
            c->iov = newbuf;
            c->iovsize = IOV_LIST_INITIAL;
        }
    /* TODO check return value */
    }
}

/**
 * Convert a state name to a human readable form.
 */
static const char *state_text(enum conn_states state) {
    const char* const statenames[] = { "conn_listening",
                                       "conn_new_cmd",
                                       "conn_waiting",
                                       "conn_read",
                                       "conn_parse_cmd",
                                       "conn_write",
                                       "conn_nread",
                                       "conn_swallow",
                                       "conn_closing",
                                       "conn_mwrite",
                                       "conn_closed" };
    return statenames[state];
}

/*
 * Sets a connection's current state in the state machine. Any special
 * processing that needs to happen on certain state transitions can
 * happen here.
 */
static void conn_set_state(conn *c, enum conn_states state) {
    assert(c != NULL);
    assert(state >= conn_listening && state < conn_max_state);

    if (state != c->state) {
        if (settings.verbose > 2) {
            fprintf(stderr, "%d: going from %s to %s\n",
                    c->sfd, state_text(c->state),
                    state_text(state));
        }

        if (state == conn_write || state == conn_mwrite) {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }
        c->state = state;
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
    assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            return -1;
        }
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    assert(c != NULL);

    do {
        m = &c->msglist[c->msgused - 1];

        if (m->msg_iov == NULL) {
            m->msg_iov = &c->iov[c->iovused];
        }

        /*
         * Limit UDP packets, and the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

        /* We may need to start a new msghdr if this one is full. */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE) {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        } else {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}


/*
 * Constructs a set of UDP headers and attaches them to the outgoing messages.
 */
static int build_udp_headers(conn *c) {
    int i;
    unsigned char *hdr;

    assert(c != NULL);

    if (c->msgused > c->hdrsize) {
        void *new_hdrbuf;
        if (c->hdrbuf) {
            new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
        } else {
            new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);
        }

        if (! new_hdrbuf) {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            return -1;
        }
        c->hdrbuf = (unsigned char *)new_hdrbuf;
        c->hdrsize = c->msgused * 2;
    }

    hdr = c->hdrbuf;
    for (i = 0; i < c->msgused; i++) {
        c->msglist[i].msg_iov[0].iov_base = (void*)hdr;
        c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
        *hdr++ = c->request_id / 256;
        *hdr++ = c->request_id % 256;
        *hdr++ = i / 256;
        *hdr++ = i % 256;
        *hdr++ = c->msgused / 256;
        *hdr++ = c->msgused % 256;
        *hdr++ = 0;
        *hdr++ = 0;
        assert((void *) hdr == (caddr_t)c->msglist[i].msg_iov[0].iov_base + UDP_HEADER_SIZE);
    }

    return 0;
}


static void out_string(conn *c, const char *str) {
    size_t len;

    assert(c != NULL);

    if (c->noreply) {
        if (settings.verbose > 1)
            fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
        c->noreply = false;
        conn_set_state(c, conn_new_cmd);
        return;
    }

    if (settings.verbose > 1)
        fprintf(stderr, ">%d %s\n", c->sfd, str);

    /* Nuke a partial output... */
    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    add_msghdr(c);

    len = strlen(str);
    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
    return;
}

/*
 * Outputs a protocol-specific "out of memory" error. For ASCII clients,
 * this is equivalent to out_string().
 */
static void out_of_memory(conn *c, char *ascii_error) {
    const static char error_prefix[] = "SERVER_ERROR ";
    const static int error_prefix_len = sizeof(error_prefix) - 1;

    if (c->protocol == binary_prot) {
        /* Strip off the generic error prefix; it's irrelevant in binary */
        if (!strncmp(ascii_error, error_prefix, error_prefix_len)) {
            ascii_error += error_prefix_len;
        }
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, ascii_error, 0);
    } else {
        out_string(c, ascii_error);
    }
}

/*
 * we get here after reading the value in set/add/replace commands. The command
 * has been stored in c->cmd, and the item is ready in c->item.
 */
static void complete_nread_ascii(conn *c) {
    assert(c != NULL);

    item *it = c->item;
    int comm = c->cmd;
    enum store_item_type ret;

    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);

    /* XXX */
    if (0 && strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
      ret = store_item(it, comm, c);

#ifdef ENABLE_DTRACE
      uint64_t cas = ITEM_get_cas(it);
      switch (c->cmd) {
      case NREAD_ADD:
          MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_REPLACE:
          MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_APPEND:
          MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                   (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_PREPEND:
          MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_SET:
          MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_CAS:
          MEMCACHED_COMMAND_CAS(c->sfd, ITEM_key(it), it->nkey, it->nbytes,
                                cas);
          break;
      }
#endif

      switch (ret) {
      case STORED:
          out_string(c, "STORED");
          break;
      case EXISTS:
          out_string(c, "EXISTS");
          break;
      case NOT_FOUND:
          out_string(c, "NOT_FOUND");
          break;
      case NOT_STORED:
          out_string(c, "NOT_STORED");
          break;
      default:
          out_string(c, "SERVER_ERROR Unhandled storage type.");
      }

    }

    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(conn *c) {
    char *ret = c->rcurr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    assert(ret >= c->rbuf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(conn *c) {
    return c->rcurr - (c->binary_header.request.keylen);
}

static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len) {
    protocol_binary_response_header* header;

    assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        /* This should never run out of memory because iov and msg lists
         * have minimum sizes big enough to hold an error response.
         */
        out_of_memory(c, "SERVER_ERROR out of memory adding binary header");
        return;
    }

    header = (protocol_binary_response_header *)c->wbuf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = htonll(c->cas);

    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, ">%d Writing bin response:", c->sfd);
        for (ii = 0; ii < sizeof(header->bytes); ++ii) {
            if (ii % 4 == 0) {
                fprintf(stderr, "\n>%d  ", c->sfd);
            }
            fprintf(stderr, " 0x%02x", header->bytes[ii]);
        }
        fprintf(stderr, "\n");
    }

    add_iov(c, c->wbuf, sizeof(header->response));
}

/**
 * Writes a binary error response. If errstr is supplied, it is used as the
 * error text; otherwise a generic description of the error status code is
 * included.
 */
static void write_bin_error(conn *c, protocol_binary_response_status err,
                            const char *errstr, int swallow) {
    size_t len;

    if (!errstr) {
        switch (err) {
        case PROTOCOL_BINARY_RESPONSE_ENOMEM:
            errstr = "Out of memory";
            break;
        case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
            errstr = "Unknown command";
            break;
        case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
            errstr = "Not found";
            break;
        case PROTOCOL_BINARY_RESPONSE_EINVAL:
            errstr = "Invalid arguments";
            break;
        case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
            errstr = "Data exists for key.";
            break;
        case PROTOCOL_BINARY_RESPONSE_E2BIG:
            errstr = "Too large.";
            break;
        case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
            errstr = "Non-numeric server-side value for incr or decr";
            break;
        case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
            errstr = "Not stored.";
            break;
        case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
            errstr = "Auth failure.";
            break;
        default:
            assert(false);
            errstr = "UNHANDLED ERROR";
            fprintf(stderr, ">%d UNHANDLED ERROR: %d\n", c->sfd, err);
        }
    }

    if (settings.verbose > 1) {
        fprintf(stderr, ">%d Writing an error: %s\n", c->sfd, errstr);
    }

    len = strlen(errstr);
    add_bin_header(c, err, 0, 0, len);
    if (len > 0) {
        add_iov(c, errstr, len);
    }
    conn_set_state(c, conn_mwrite);
    if(swallow > 0) {
        c->sbytes = swallow;
        c->write_and_go = conn_swallow;
    } else {
        c->write_and_go = conn_new_cmd;
    }
}

/* Form and send a response to a command over the binary protocol */
static void write_bin_response(conn *c, void *d, int hlen, int keylen, int dlen) {
    if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET ||
        c->cmd == PROTOCOL_BINARY_CMD_GETK) {
        add_bin_header(c, 0, hlen, keylen, dlen);
        if(dlen > 0) {
            add_iov(c, d, dlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
    } else {
        conn_set_state(c, conn_new_cmd);
    }
}

static void complete_incr_bin(conn *c) {
    item *it;
    char *key;
    size_t nkey;
    /* Weird magic in add_delta forces me to pad here */
    char tmpbuf[INCR_MAX_STORAGE_LEN];
    uint64_t cas = 0;

    protocol_binary_response_incr* rsp = (protocol_binary_response_incr*)c->wbuf;
    protocol_binary_request_incr* req = binary_get_request(c);

    assert(c != NULL);
    assert(c->wsize >= sizeof(*rsp));

    /* fix byteorder in the request */
    req->message.body.delta = ntohll(req->message.body.delta);
    req->message.body.initial = ntohll(req->message.body.initial);
    req->message.body.expiration = ntohl(req->message.body.expiration);
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        int i;
        fprintf(stderr, "incr ");

        for (i = 0; i < nkey; i++) {
            fprintf(stderr, "%c", key[i]);
        }
        fprintf(stderr, " %lld, %llu, %d\n",
                (long long)req->message.body.delta,
                (long long)req->message.body.initial,
                req->message.body.expiration);
    }

    if (c->binary_header.request.cas != 0) {
        cas = c->binary_header.request.cas;
    }
    switch(add_delta(c, key, nkey, c->cmd == PROTOCOL_BINARY_CMD_INCREMENT,
                     req->message.body.delta, tmpbuf,
                     &cas)) {
    case OK:
        rsp->message.body.value = htonll(strtoull(tmpbuf, NULL, 10));
        if (cas) {
            c->cas = cas;
        }
        write_bin_response(c, &rsp->message.body, 0, 0,
                           sizeof(rsp->message.body.value));
        break;
    case NON_NUMERIC:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, NULL, 0);
        break;
    case EOM:
        out_of_memory(c, "SERVER_ERROR Out of memory incrementing value");
        break;
    case DELTA_ITEM_NOT_FOUND:
        if (req->message.body.expiration != 0xffffffff) {
            /* Save some room for the response */
            rsp->message.body.value = htonll(req->message.body.initial);

            snprintf(tmpbuf, INCR_MAX_STORAGE_LEN, "%llu",
                (unsigned long long)req->message.body.initial);
            int res = strlen(tmpbuf);
            it = item_alloc(key, nkey, 0, realtime(req->message.body.expiration),
                            res + 2);

            if (it != NULL) {
                memcpy(ITEM_data(it), tmpbuf, res);
                memcpy(ITEM_data(it) + res, "\r\n", 2);

                if (store_item(it, NREAD_ADD, c)) {
                    c->cas = ITEM_get_cas(it);
                    write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body.value));
                } else {
                    write_bin_error(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED,
                                    NULL, 0);
                }
                item_remove(it);         /* release our reference */
            } else {
                out_of_memory(c,
                        "SERVER_ERROR Out of memory allocating new item");
            }
        } else {
            pthread_mutex_lock(&c->thread->stats.mutex);
            if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
                c->thread->stats.incr_misses++;
            } else {
                c->thread->stats.decr_misses++;
            }
            pthread_mutex_unlock(&c->thread->stats.mutex);

            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
        }
        break;
    case DELTA_ITEM_CAS_MISMATCH:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);
        break;
    }
}

static void complete_update_bin(conn *c) {
    protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    enum store_item_type ret = NOT_STORED;
    assert(c != NULL);

    item *it = c->item;

    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    *(ITEM_data(it) + it->nbytes - 2) = '\r';
    *(ITEM_data(it) + it->nbytes - 1) = '\n';

    ret = store_item(it, c->cmd, c);

#ifdef ENABLE_DTRACE
    uint64_t cas = ITEM_get_cas(it);
    switch (c->cmd) {
    case NREAD_ADD:
        MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_REPLACE:
        MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                  (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_APPEND:
        MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_PREPEND:
        MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_SET:
        MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    }
#endif

    switch (ret) {
    case STORED:
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case EXISTS:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);
        break;
    case NOT_FOUND:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
        break;
    case NOT_STORED:
        if (c->cmd == NREAD_ADD) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        } else if(c->cmd == NREAD_REPLACE) {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        } else {
            eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        }
        write_bin_error(c, eno, NULL, 0);
    }

    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

static void process_bin_get_or_touch(conn *c) {
    item *it;

    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    int should_touch = (c->cmd == PROTOCOL_BINARY_CMD_TOUCH ||
                        c->cmd == PROTOCOL_BINARY_CMD_GAT ||
                        c->cmd == PROTOCOL_BINARY_CMD_GATK);
    int should_return_key = (c->cmd == PROTOCOL_BINARY_CMD_GETK ||
                             c->cmd == PROTOCOL_BINARY_CMD_GATK);
    int should_return_value = (c->cmd != PROTOCOL_BINARY_CMD_TOUCH);

    if (settings.verbose > 1) {
        fprintf(stderr, "<%d %s ", c->sfd, should_touch ? "TOUCH" : "GET");
        fwrite(key, 1, nkey, stderr);
        fputc('\n', stderr);
    }

    if (should_touch) {
        protocol_binary_request_touch *t = binary_get_request(c);
        time_t exptime = ntohl(t->message.body.expiration);

        it = item_touch(key, nkey, realtime(exptime));
    } else {
        it = item_get(key, nkey);
    }

    if (it) {
        /* the length has two unnecessary bytes ("\r\n") */
        uint16_t keylen = 0;
        uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

        item_update(it);
        pthread_mutex_lock(&c->thread->stats.mutex);
        if (should_touch) {
            c->thread->stats.touch_cmds++;
            c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
        } else {
            c->thread->stats.get_cmds++;
            c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
        }
        pthread_mutex_unlock(&c->thread->stats.mutex);

        if (should_touch) {
            MEMCACHED_COMMAND_TOUCH(c->sfd, ITEM_key(it), it->nkey,
                                    it->nbytes, ITEM_get_cas(it));
        } else {
            MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                  it->nbytes, ITEM_get_cas(it));
        }

        if (c->cmd == PROTOCOL_BINARY_CMD_TOUCH) {
            bodylen -= it->nbytes - 2;
        } else if (should_return_key) {
            bodylen += nkey;
            keylen = nkey;
        }

        add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
        rsp->message.header.response.cas = htonll(ITEM_get_cas(it));

        // add the flags
        rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        if (should_return_key) {
            add_iov(c, ITEM_key(it), nkey);
        }

        if (should_return_value) {
            /* Add the data minus the CRLF */
            add_iov(c, ITEM_data(it), it->nbytes - 2);
        }

        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        /* Remember this command so we can garbage collect it later */
        c->item = it;
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        if (should_touch) {
            c->thread->stats.touch_cmds++;
            c->thread->stats.touch_misses++;
        } else {
            c->thread->stats.get_cmds++;
            c->thread->stats.get_misses++;
        }
        pthread_mutex_unlock(&c->thread->stats.mutex);

        if (should_touch) {
            MEMCACHED_COMMAND_TOUCH(c->sfd, key, nkey, -1, 0);
        } else {
            MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
        }

        if (c->noreply) {
            conn_set_state(c, conn_new_cmd);
        } else {
            if (should_return_key) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                        0, nkey, nkey);
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                                NULL, 0);
            }
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_get(key, nkey, NULL != it);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->stats.buffer + c->stats.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header = {
        .response.magic = (uint8_t)PROTOCOL_BINARY_RES,
        .response.opcode = PROTOCOL_BINARY_CMD_STAT,
        .response.keylen = (uint16_t)htons(klen),
        .response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
        .response.bodylen = htonl(bodylen),
        .response.opaque = c->opaque
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               conn *c) {
    char *pos = c->stats.buffer + c->stats.offset;
    uint32_t nbytes = 0;
    int remaining = c->stats.size - c->stats.offset;
    int room = remaining - 1;

    if (klen == 0 && vlen == 0) {
        nbytes = snprintf(pos, room, "END\r\n");
    } else if (vlen == 0) {
        nbytes = snprintf(pos, room, "STAT %s\r\n", key);
    } else {
        nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);
    }

    c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn *c, size_t needed) {
    size_t nsize = c->stats.size;
    size_t available = nsize - c->stats.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->stats.buffer == NULL) {
        nsize = 1024;
        available = c->stats.size = c->stats.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->stats.offset;
    }

    if (nsize != c->stats.size) {
        char *ptr = realloc(c->stats.buffer, nsize);
        if (ptr) {
            c->stats.buffer = ptr;
            c->stats.size = nsize;
        } else {
            STATS_LOCK();
            stats.malloc_fails++;
            STATS_UNLOCK();
            rv = false;
        }
    }

    return rv;
}

static void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    conn *c = (conn*)cookie;

    if (c->protocol == binary_prot) {
        size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_bin_stats(key, klen, val, vlen, c);
    } else {
        size_t needed = vlen + klen + 10; // 10 == "STAT = \r\n"
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_ascii_stats(key, klen, val, vlen, c);
    }

    assert(c->stats.offset <= c->stats.size);
}

static void process_bin_stat(conn *c) {
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, "<%d STATS ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", subcommand[ii]);
        }
        fprintf(stderr, "\n");
    }

    if (nkey == 0) {
        /* request all statistics */
        server_stats(&append_stats, c);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strncmp(subcommand, "reset", 5) == 0) {
        stats_reset();
    } else if (strncmp(subcommand, "settings", 8) == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strncmp(subcommand, "detail", 6) == 0) {
        char *subcmd_pos = subcommand + 6;
        if (strncmp(subcmd_pos, " dump", 5) == 0) {
            int len;
            char *dump_buf = stats_prefix_dump(&len);
            if (dump_buf == NULL || len <= 0) {
                out_of_memory(c, "SERVER_ERROR Out of memory generating stats");
                return ;
            } else {
                append_stats("detailed", strlen("detailed"), dump_buf, len, c);
                free(dump_buf);
            }
        } else if (strncmp(subcmd_pos, " on", 3) == 0) {
            settings.detail_enabled = 1;
        } else if (strncmp(subcmd_pos, " off", 4) == 0) {
            settings.detail_enabled = 0;
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
            return;
        }
    } else {
        if (get_stats(subcommand, nkey, &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                out_of_memory(c, "SERVER_ERROR Out of memory generating stats");
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
        }

        return;
    }

    /* Append termination package and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);
    if (c->stats.buffer == NULL) {
        out_of_memory(c, "SERVER_ERROR Out of memory preparing to send stats");
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

static void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
    assert(c);
    c->substate = next_substate;
    c->rlbytes = c->keylen + extra;

    /* Ok... do we have room for the extras and the key in the input buffer? */
    ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
    if (c->rlbytes > c->rsize - offset) {
        size_t nsize = c->rsize;
        size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->rsize) {
            if (settings.verbose > 1) {
                fprintf(stderr, "%d: Need to grow buffer from %lu to %lu\n",
                        c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
            }
            char *newm = realloc(c->rbuf, nsize);
            if (newm == NULL) {
                STATS_LOCK();
                stats.malloc_fails++;
                STATS_UNLOCK();
                if (settings.verbose) {
                    fprintf(stderr, "%d: Failed to grow buffer.. closing connection\n",
                            c->sfd);
                }
                conn_set_state(c, conn_closing);
                return;
            }

            c->rbuf= newm;
            /* rcurr should point to the same offset in the packet */
            c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
            c->rsize = nsize;
        }
        if (c->rbuf != c->rcurr) {
            memmove(c->rbuf, c->rcurr, c->rbytes);
            c->rcurr = c->rbuf;
            if (settings.verbose > 1) {
                fprintf(stderr, "%d: Repack input buffer\n", c->sfd);
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
    conn_set_state(c, conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(conn *c) {
    write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, NULL, 0);
    if (settings.verbose) {
        fprintf(stderr, "Protocol error (opcode %02x), close connection %d\n",
                c->binary_header.request.opcode, c->sfd);
    }
    c->write_and_go = conn_closing;
}

static void init_sasl_conn(conn *c) {
    assert(c);
    /* should something else be returned? */
    if (!settings.sasl)
        return;

    c->authenticated = false;

    if (!c->sasl_conn) {
        int result=sasl_server_new("memcached",
                                   NULL,
                                   my_sasl_hostname[0] ? my_sasl_hostname : NULL,
                                   NULL, NULL,
                                   NULL, 0, &c->sasl_conn);
        if (result != SASL_OK) {
            if (settings.verbose) {
                fprintf(stderr, "Failed to initialize SASL conn.\n");
            }
            c->sasl_conn = NULL;
        }
    }
}

static void bin_list_sasl_mechs(conn *c) {
    // Guard against a disabled SASL.
    if (!settings.sasl) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
                        c->binary_header.request.bodylen
                        - c->binary_header.request.keylen);
        return;
    }

    init_sasl_conn(c);
    const char *result_string = NULL;
    unsigned int string_length = 0;
    int result=sasl_listmech(c->sasl_conn, NULL,
                             "",   /* What to prepend the string with */
                             " ",  /* What to separate mechanisms with */
                             "",   /* What to append to the string */
                             &result_string, &string_length,
                             NULL);
    if (result != SASL_OK) {
        /* Perhaps there's a better error for this... */
        if (settings.verbose) {
            fprintf(stderr, "Failed to list SASL mechanisms.\n");
        }
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
        return;
    }
    write_bin_response(c, (char*)result_string, 0, 0, string_length);
}

static void process_bin_sasl_auth(conn *c) {
    // Guard for handling disabled SASL on the server.
    if (!settings.sasl) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
                        c->binary_header.request.bodylen
                        - c->binary_header.request.keylen);
        return;
    }

    assert(c->binary_header.request.extlen == 0);

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    if (nkey > MAX_SASL_MECH_LEN) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, NULL, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    char *key = binary_get_key(c);
    assert(key);

    item *it = item_alloc(key, nkey, 0, 0, vlen);

    if (it == 0) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, NULL, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_reading_sasl_auth_data;
}

static void process_bin_complete_sasl_auth(conn *c) {
    assert(settings.sasl);
    const char *out = NULL;
    unsigned int outlen = 0;

    assert(c->item);
    init_sasl_conn(c);

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    char mech[nkey+1];
    memcpy(mech, ITEM_key((item*)c->item), nkey);
    mech[nkey] = 0x00;

    if (settings.verbose)
        fprintf(stderr, "mech:  ``%s'' with %d bytes of data\n", mech, vlen);

    const char *challenge = vlen == 0 ? NULL : ITEM_data((item*) c->item);

    int result=-1;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        result = sasl_server_start(c->sasl_conn, mech,
                                   challenge, vlen,
                                   &out, &outlen);
        break;
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        result = sasl_server_step(c->sasl_conn,
                                  challenge, vlen,
                                  &out, &outlen);
        break;
    default:
        assert(false); /* CMD should be one of the above */
        /* This code is pretty much impossible, but makes the compiler
           happier */
        if (settings.verbose) {
            fprintf(stderr, "Unhandled command %d with challenge %s\n",
                    c->cmd, challenge);
        }
        break;
    }

    item_unlink(c->item);

    if (settings.verbose) {
        fprintf(stderr, "sasl result code:  %d\n", result);
    }

    switch(result) {
    case SASL_OK:
        c->authenticated = true;
        write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        break;
    case SASL_CONTINUE:
        add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0, outlen);
        if(outlen > 0) {
            add_iov(c, out, outlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    default:
        if (settings.verbose)
            fprintf(stderr, "Unknown sasl response:  %d\n", result);
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        c->thread->stats.auth_errors++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
    }
}

static bool authenticated(conn *c) {
    assert(settings.sasl);
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
        rv = true;
        break;
    default:
        rv = c->authenticated;
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "authenticated() in cmd 0x%02x is %s\n",
                c->cmd, rv ? "true" : "false");
    }

    return rv;
}

static void dispatch_bin_command(conn *c) {
    int protocol_error = 0;

    int extlen = c->binary_header.request.extlen;
    int keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (settings.sasl && !authenticated(c)) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
        c->write_and_go = conn_closing;
        return;
    }

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
    c->noreply = true;

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SETQ:
        c->cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case PROTOCOL_BINARY_CMD_ADDQ:
        c->cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case PROTOCOL_BINARY_CMD_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
        break;
    case PROTOCOL_BINARY_CMD_QUITQ:
        c->cmd = PROTOCOL_BINARY_CMD_QUIT;
        break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
        break;
    case PROTOCOL_BINARY_CMD_APPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;
    case PROTOCOL_BINARY_CMD_GETQ:
        c->cmd = PROTOCOL_BINARY_CMD_GET;
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GETK;
        break;
    case PROTOCOL_BINARY_CMD_GATQ:
        c->cmd = PROTOCOL_BINARY_CMD_GAT;
        break;
    case PROTOCOL_BINARY_CMD_GATKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GAT;
        break;
    default:
        c->noreply = false;
    }

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, VERSION, 0, 0, strlen(VERSION));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
                bin_read_key(c, bin_reading_set_header, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETK:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                bin_read_key(c, bin_reading_get_key, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                bin_read_key(c, bin_reading_del_header, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_incr_header, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                bin_read_key(c, bin_reading_set_header, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_STAT:
            if (extlen == 0) {
                bin_read_key(c, bin_reading_stat, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_QUIT:
            if (keylen == 0 && extlen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
                c->write_and_go = conn_closing;
                if (c->noreply) {
                    conn_set_state(c, conn_closing);
                }
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                bin_list_sasl_mechs(c);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_AUTH:
        case PROTOCOL_BINARY_CMD_SASL_STEP:
            if (extlen == 0 && keylen != 0) {
                bin_read_key(c, bin_reading_sasl_auth, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_TOUCH:
        case PROTOCOL_BINARY_CMD_GAT:
        case PROTOCOL_BINARY_CMD_GATQ:
        case PROTOCOL_BINARY_CMD_GATK:
        case PROTOCOL_BINARY_CMD_GATKQ:
            if (extlen == 4 && keylen != 0) {
                bin_read_key(c, bin_reading_touch_key, 4);
            } else {
                protocol_error = 1;
            }
            break;
        default:
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, NULL,
                            bodylen);
    }

    if (protocol_error)
        handle_binary_protocol_error(c);
}

static void process_bin_update(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    req->message.body.flags = ntohl(req->message.body.flags);
    req->message.body.expiration = ntohl(req->message.body.expiration);

    vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);

    if (settings.verbose > 1) {
        int ii;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            fprintf(stderr, "<%d ADD ", c->sfd);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            fprintf(stderr, "<%d SET ", c->sfd);
        } else {
            fprintf(stderr, "<%d REPLACE ", c->sfd);
        }
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }

        fprintf(stderr, " Value len is %d", vlen);
        fprintf(stderr, "\n");
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, req->message.body.flags,
            realtime(req->message.body.expiration), vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, req->message.body.flags, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, NULL, vlen);
        } else {
            out_of_memory(c, "SERVER_ERROR Out of memory allocating item");
        }

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->cmd = NREAD_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            c->cmd = NREAD_SET;
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            c->cmd = NREAD_REPLACE;
            break;
        default:
            assert(0);
    }

    if (ITEM_get_cas(it) != 0) {
        c->cmd = NREAD_CAS;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_append_prepend(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    vlen = c->binary_header.request.bodylen - nkey;

    if (settings.verbose > 1) {
        fprintf(stderr, "Value len is %d\n", vlen);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, 0, 0, vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, 0, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, NULL, vlen);
        } else {
            out_of_memory(c, "SERVER_ERROR Out of memory allocating item");
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_APPEND:
            c->cmd = NREAD_APPEND;
            break;
        case PROTOCOL_BINARY_CMD_PREPEND:
            c->cmd = NREAD_PREPEND;
            break;
        default:
            assert(0);
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_flush(conn *c) {
    time_t exptime = 0;
    protocol_binary_request_flush* req = binary_get_request(c);

    if (!settings.flush_enabled) {
      // flush_all is not allowed but we log it on stats
      write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, NULL, 0);
      return;
    }

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (exptime > 0) {
        settings.oldest_live = realtime(exptime) - 1;
    } else {
        settings.oldest_live = current_time - 1;
    }
    item_flush_expired();

    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.flush_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);

    write_bin_response(c, NULL, 0, 0, 0);
}

static void process_bin_delete(conn *c) {
    item *it;

    protocol_binary_request_delete* req = binary_get_request(c);

    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    assert(c != NULL);

    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, "Deleting ");
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, "\n");
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        uint64_t cas = ntohll(req->message.header.request.cas);
        if (cas == 0 || cas == ITEM_get_cas(it)) {
            MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            item_unlink(it);
            write_bin_response(c, NULL, 0, 0, 0);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, NULL, 0);
        }
        item_remove(it);      /* release our reference */
    } else {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, NULL, 0);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
    }
}

static void complete_nread_binary(conn *c) {
    assert(c != NULL);
    assert(c->cmd >= 0);

    switch(c->substate) {
    case bin_reading_set_header:
        if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
                c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
            process_bin_append_prepend(c);
        } else {
            process_bin_update(c);
        }
        break;
    case bin_read_set_value:
        complete_update_bin(c);
        break;
    case bin_reading_get_key:
    case bin_reading_touch_key:
        process_bin_get_or_touch(c);
        break;
    case bin_reading_stat:
        process_bin_stat(c);
        break;
    case bin_reading_del_header:
        process_bin_delete(c);
        break;
    case bin_reading_incr_header:
        complete_incr_bin(c);
        break;
    case bin_read_flush_exptime:
        process_bin_flush(c);
        break;
    case bin_reading_sasl_auth:
        process_bin_sasl_auth(c);
        break;
    case bin_reading_sasl_auth_data:
        process_bin_complete_sasl_auth(c);
        break;
    default:
        fprintf(stderr, "Not handling substate %d\n", c->substate);
        assert(0);
    }
}

static void reset_cmd_handler(conn *c) {
    debug("reset_cmd_handler\n");
    c->cmd = -1;
    c->substate = bin_no_state;
    if(c->item != NULL) {
        item_remove(c->item);
        c->item = NULL;
    }
    conn_shrink(c);
    if (c->rbytes > 0) {
        debug("reset_cmd_handler: to conn_parse_cmd\n");
        conn_set_state(c, conn_parse_cmd);
    } else {
        debug("reset_cmd_handler: to conn_waiting\n");
        conn_set_state(c, conn_waiting);
    }
}

static void complete_recovery_scatter_nread(conn *c)
{
    struct recovery_queue_item *it = c->rqit;
    fill_completed_recovered_data(it->unit_begin, it->unit_end, c->vbuf);
    free(c->vbuf);
    c->vbuf = NULL;
    recovery_req_remove(&recovery, c->rqit);
    conn_set_state(c, conn_new_cmd);
}

static int
check_recovery_1st_completeness(struct recovery_queue_item *rqit)
{
    if ((rqit->mask & (1<<settings.lid)) == 0) return 1;
    uint32_t check_mask = (1<<settings.nshard)-1;
    struct recovery *r = &recovery;
    struct recovery_queue_item *it = rqit;
    int i;
    for (i=it->unit_begin; i<=it->unit_end; ++i) {
        struct recovery_unit *unit = &r->units[i];
        // retrive flags w/o status bits
        uint32_t flags = unit->flags & (~((1<<30)|(1<<31)));
        // no bad guy who sent me his data
        assert((flags & (~it->mask)) == 0);

        debug(" 1st completeness: %x %x\n",
                flags & check_mask, it->mask & check_mask);
        if ((flags & check_mask) != (it->mask & check_mask)) {
            return 0;
        }
    }
    return 1;
}

static int
check_recovery_2nd_completeness(struct recovery_queue_item *rqit)
{
    struct recovery_queue_item *it = rqit;

    int i;

    if (it->data_from_parity == NULL) return 0;

    FOREACH_PARITY_LID(i) {
        if (i == settings.lid) continue;
        if ((it->mask & (1<<i)) && ((it->flags & (1<<i)) == 0)) {
            return 0;
        }
    }
    return 1;
}

static void complete_recovery_gather_nread(conn *c)
{
    if (c->rqit == NULL) {
        if (c->vbuf) free(c->vbuf);
        c->vbuf = NULL;
        conn_set_state(c, conn_new_cmd);
        return;
    }
    // detach vbuf
    c->vbuf = NULL;

    c->rqit->flags |= 1 << get_real_peer_lid_by_conn(c);

    // check only the shards
    int complete = check_recovery_1st_completeness(c->rqit);
    // TODO: do sth after complete
    if (complete) {
        debug("complete\n");
        assert(recovery_leader == settings.lid);
        // i am the leader
        complete = check_recovery_2nd_completeness(c->rqit);
        if (complete) {
            // TODO: complete bh
            debug("COMPLETE both\n");
            complete_recovery_bottom_half(c->rqit);
            recovery_req_remove(&recovery, c->rqit);
            //assert_units_recovered(ubegin, uend);
        }
    }
    conn_set_state(c, conn_new_cmd);
}


static void complete_recovery_nread(conn *c)
{
    assert(c->rqit);
    struct recovery_queue_item *it = c->rqit;

    if (it->in_use == -1) {
        // the recover request is already aborted
        // and a new recover request is already been issued
        debug("ignore restarted recover request\n");
        recovery_req_remove(&recovery, c->rqit);
        conn_set_state(c, conn_new_cmd);
        return;
    }

    // retrive from queue_item
    int ubegin = it->unit_begin;
    int uend = it->unit_end;
    uint32_t mask = it->mask;
    // FIXME: how to use maxxid?
    //uint64_t maxxid = c->xid;
    int leader_lid = it->leader_lid;
    int lid = get_peer_lid_by_conn(c);

    debug("recovery_recover_units [%d,%d] mask=%x leader=%d from=%d\n",
            ubegin, uend, mask, leader_lid, lid);
    recovery_recover_units(&recovery, &ecmem, lid,
            ubegin, uend, c->vbuf);
    debug("recovery_recover_units done\n");

    // check only the shards
    int complete = check_recovery_1st_completeness(c->rqit);
    // TODO: do sth after complete
    if (complete) {
        debug("complete\n");
        if (leader_lid == settings.lid) {
            // i am the leader
            complete = check_recovery_2nd_completeness(c->rqit);
            if (complete) {
                // TODO: complete bh
                debug("COMPLETE both\n");
                complete_recovery_bottom_half(c->rqit);
                recovery_req_remove(&recovery, c->rqit);
                //assert_units_recovered(ubegin, uend);
                //assert(0);
            }
        } else {
            debug("COMPLETE first step\n");
            //send_data_to_leader
            send_recovered_data(leader_lid, ubegin, uend, mask);
            // the removal of req will be done after
            //  the finally recovered data are received
            //recovery_req_remove(&recovery, rid);
        }
    }
    conn_set_state(c, conn_new_cmd);
}

static void complete_nread(conn *c) {
    assert(c != NULL);
    assert(c->protocol == ascii_prot
           || c->protocol == binary_prot);


#ifdef ENABLE_COCYTUS
    debug("complete_nread\n");
    item *it = c->item;
    int lid;

    if (!IS_PARITY_SERVER) {
        // data process set operation
        // the addr we get here is already 16-aligned
        uint64_t addr = ecmem_alloc(&ecmem, it->nbytes);
        debug(BG_G"ec_alloc get %"PRIu64" in compelte_nread, normal set\n"BG_D, addr);
        assert(addr % 16 == 0);
        char *addrp = ecmem_get(&ecmem, addr);

        // alloc for diff
        char *diff_base  = safe_malloc(it->nbytes + 15 + 128);
        // align to 16
        char *diff = ROUND(diff_base, 16ul);

        // move new data to diff
        memcpy(diff, c->vbuf, it->nbytes);

        // calc the real diff
        galois_w08_region_multiply(addrp, 1, it->nbytes, diff, 1);

        it->addr = addr;

        // send to others
        c->xid = alloc_xid++;
        struct rep_queue_item *rqit = rep_queue_add(rep_queue);
        rqit->xid = c->xid;
        rqit->conn_ptr = (uint64_t)c;
        rqit->done = 0;
        c->ack = 0;
        int i;
        FOREACH_PARITY_LID(i) {
            if (peers[i].state == peer_lost) {
                continue;
            }
            c->ack |= 1 << i;
            parity_send(peers[i].conn_w, c, c->xid, it, diff);
            event_active(&peers[i].conn_w->event, EV_WRITE, 0);
        }

        free(diff_base);
        switch_hashtable(0);

        if (c->ack == 0) {
            // no alive parity
            conn_set_state(c, conn_waiting_ack);
        } else {
            c->next_state = conn_waiting_ack;
            conn_set_state(c, conn_pre_waiting_ack);
        }

        return;

    } else if (c->lid == sub_as_lid) {
        // parity handles the set
        // the addr we get here is already 16-aligned
        switch_hashtable(sub_as_lid);
        uint64_t addr = ec_alloc(ecalloc, it->nbytes);
        debug(BG_G"ec_alloc get %"PRIu64" in compelte_nread, parity handles set\n"BG_D, addr);
        assert(addr % 16 == 0);
        it->addr = addr; // store the addr
        uint64_t size = it->nbytes;

        int ubegin = recovery_calc_unit_id(&recovery, addr);
        int uend = recovery_calc_unit_id(&recovery, addr + size - 1);

        c->item = it;
        int needrec = need_recovery(ubegin, uend);
        if (!needrec) {
            conn_set_state(c, conn_recovery_complete_for_set);
            return;
        }

        try_do_recovery(c, ubegin, uend, conn_recovery_complete_for_set);
        //try_do_recovery(NULL, ubegin, uend, conn_recovery_complete_for_set);

        //ec_free(ecalloc, addr);
        //conn_set_state(c, conn_closing);
        //return;

        int i;
        FOREACH_PARITY_LID(i) {
            if (i == settings.lid) {
                //already done
                //uint64_t myaddr = ec_alloc(&ecallocs[sub_as_lid], it->nbytes);
                //myaddr = myaddr;
                //assert(addr == myaddr);
            } else {
                debug("send pre_alloc %"PRIu64" %d for %d to %d\n",
                        addr, it->nbytes, sub_as_lid, i);
                send_msgf_raw(peers[i].conn_w,
                        "pre_alloc %"PRIu64" %d %"PRIu64,
                        addr, it->nbytes, stable_xid);
            }
        }
        return;

    } else {
        // rep nread
        lid = get_peer_lid_by_conn(c);
        struct rep_queue_item *e = rep_queue_find(peers[lid].rep_queue, c->xid);
        assert(e != NULL);
        // FIXME
        debug("e->lid %d ; lid %d; c->xid %"PRIu64" ; e->xid %"PRIu64"\n",
                e->lid, lid, c->xid, e->xid);
        assert(e->lid == lid);
        debug("repack STORED %p %"PRIu64 "\n", (void *)e->conn_ptr, c->xid);
        lid = get_real_peer_lid_by_conn(c);
        debug("new lid %d\n", lid);
        send_msgf(peers[lid].conn_w, "repack STORED %" PRIu64 " %"PRIu64, e->conn_ptr, c->xid);
        e->ack = 1;
        event_active(&peers[lid].conn_w->event, EV_WRITE, 0);
        conn_set_state(c, conn_new_cmd);
        return;
    }
#endif

    if (c->protocol == ascii_prot) {
        complete_nread_ascii(c);
    } else if (c->protocol == binary_prot) {
        complete_nread_binary(c);
    }
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
enum store_item_type do_store_item(item *it, int comm, conn *c, const uint32_t hv) {
    char *key = ITEM_key(it);
    item *old_it = do_item_get(key, it->nkey, hv);
    enum store_item_type stored = NOT_STORED;

    item *new_it = NULL;
    int flags;

    if (old_it != NULL && comm == NREAD_ADD) {
        /* add only adds a nonexistent item, but promote to head of LRU */
        do_item_update(old_it);
    } else if (!old_it && (comm == NREAD_REPLACE
        || comm == NREAD_APPEND || comm == NREAD_PREPEND))
    {
        /* replace only replaces an existing value; don't store */
    } else if (comm == NREAD_CAS) {
        /* validate cas operation */
        if(old_it == NULL) {
            // LRU expired
            stored = NOT_FOUND;
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.cas_misses++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
        }
        else if (ITEM_get_cas(it) == ITEM_get_cas(old_it)) {
            // cas validates
            // it and old_it may belong to different classes.
            // I'm updating the stats for the one that's getting pushed out
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_hits++;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            item_replace(old_it, it, hv);
            stored = STORED;
        } else {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_badval++;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            if(settings.verbose > 1) {
                fprintf(stderr, "CAS:  failure: expected %llu, got %llu\n",
                        (unsigned long long)ITEM_get_cas(old_it),
                        (unsigned long long)ITEM_get_cas(it));
            }
            stored = EXISTS;
        }
    } else {
        /*
         * Append - combine new and old record into single one. Here it's
         * atomic and thread-safe.
         */
        if (comm == NREAD_APPEND || comm == NREAD_PREPEND) {
            /*
             * Validate CAS
             */
            if (ITEM_get_cas(it) != 0) {
                // CAS much be equal
                if (ITEM_get_cas(it) != ITEM_get_cas(old_it)) {
                    stored = EXISTS;
                }
            }

            if (stored == NOT_STORED) {
                /* we have it and old_it here - alloc memory to hold both */
                /* flags was already lost - so recover them from ITEM_suffix(it) */

                flags = (int) strtol(ITEM_suffix(old_it), (char **) NULL, 10);

                new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime, it->nbytes + old_it->nbytes - 2 /* CRLF */, hv);

                if (new_it == NULL) {
                    /* SERVER_ERROR out of memory */
                    if (old_it != NULL)
                        do_item_remove(old_it);

                    return NOT_STORED;
                }

                /* copy data from it and old_it to new_it */

                if (comm == NREAD_APPEND) {
                    memcpy(ITEM_data(new_it), ITEM_data(old_it), old_it->nbytes);
                    memcpy(ITEM_data(new_it) + old_it->nbytes - 2 /* CRLF */, ITEM_data(it), it->nbytes);
                } else {
                    /* NREAD_PREPEND */
                    memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);
                    memcpy(ITEM_data(new_it) + it->nbytes - 2 /* CRLF */, ITEM_data(old_it), old_it->nbytes);
                }

                it = new_it;
            }
        }

        if (stored == NOT_STORED) {

            if (old_it != NULL) {
                debug(BG_R"addr to ec_free %"PRIu64" in store_item"BG_D"\n", old_it->addr);
                ec_free(ecalloc, old_it->addr);
                item_replace(old_it, it, hv);
            }
            else
                do_item_link(it, hv);

            if (c) c->cas = ITEM_get_cas(it);

            stored = STORED;
        }
    }

    if (old_it != NULL)
        do_item_remove(old_it);         /* release our reference */
    if (new_it != NULL)
        do_item_remove(new_it);

    if (stored == STORED) {
        // FIXME: cas may be wrong
        if (c) c->cas = ITEM_get_cas(it);
    }

    return stored;
}


/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;
    size_t len = strlen(command);
    unsigned int i = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    s = e = command;
    for (i = 0; i < len; i++) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
                if (ntokens == max_tokens - 1) {
                    e++;
                    s = e; /* so we don't add an extra token */
                    break;
                }
            }
            s = e + 1;
        }
        e++;
    }

    if (s != e) {
        tokens[ntokens].value = s;
        tokens[ntokens].length = e - s;
        ntokens++;
    }

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = bytes;
        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
    } else {
        out_of_memory(c, "SERVER_ERROR out of memory writing stats");
    }
}

static inline bool set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int noreply_index = ntokens - 2;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" option is not reliable anyway, so
      it can't be helped.
    */
    if (tokens[noreply_index].value
        && strcmp(tokens[noreply_index].value, "noreply") == 0) {
        c->noreply = true;
    }
    return c->noreply;
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name);
    assert(add_stats);
    assert(c);
    assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

inline static void process_stats_detail(conn *c, const char *command) {
    assert(c != NULL);

    if (strcmp(command, "on") == 0) {
        settings.detail_enabled = 1;
        out_string(c, "OK");
    }
    else if (strcmp(command, "off") == 0) {
        settings.detail_enabled = 0;
        out_string(c, "OK");
    }
    else if (strcmp(command, "dump") == 0) {
        int len;
        char *stats = stats_prefix_dump(&len);
        write_and_free(c, stats, len);
    }
    else {
        out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
    }
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stats, conn *c) {
    pid_t pid = getpid();
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);
    struct slab_stats slab_stats;
    slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef WIN32
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif /* !WIN32 */

    STATS_LOCK();

    APPEND_STAT("pid", "%lu", (long)pid);
    APPEND_STAT("uptime", "%u", now - ITEM_UPDATE_INTERVAL);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", VERSION);
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif /* !WIN32 */

    APPEND_STAT("curr_connections", "%u", stats.curr_conns - 1);
    APPEND_STAT("total_connections", "%u", stats.total_conns);
    if (settings.maxconns_fast) {
        APPEND_STAT("rejected_connections", "%llu", (unsigned long long)stats.rejected_conns);
    }
    APPEND_STAT("connection_structures", "%u", stats.conn_structs);
    APPEND_STAT("reserved_fds", "%u", stats.reserved_fds);
    APPEND_STAT("cmd_get", "%llu", (unsigned long long)thread_stats.get_cmds);
    APPEND_STAT("cmd_set", "%llu", (unsigned long long)slab_stats.set_cmds);
    APPEND_STAT("cmd_flush", "%llu", (unsigned long long)thread_stats.flush_cmds);
    APPEND_STAT("cmd_touch", "%llu", (unsigned long long)thread_stats.touch_cmds);
    APPEND_STAT("get_hits", "%llu", (unsigned long long)slab_stats.get_hits);
    APPEND_STAT("get_misses", "%llu", (unsigned long long)thread_stats.get_misses);
    APPEND_STAT("delete_misses", "%llu", (unsigned long long)thread_stats.delete_misses);
    APPEND_STAT("delete_hits", "%llu", (unsigned long long)slab_stats.delete_hits);
    APPEND_STAT("incr_misses", "%llu", (unsigned long long)thread_stats.incr_misses);
    APPEND_STAT("incr_hits", "%llu", (unsigned long long)slab_stats.incr_hits);
    APPEND_STAT("decr_misses", "%llu", (unsigned long long)thread_stats.decr_misses);
    APPEND_STAT("decr_hits", "%llu", (unsigned long long)slab_stats.decr_hits);
    APPEND_STAT("cas_misses", "%llu", (unsigned long long)thread_stats.cas_misses);
    APPEND_STAT("cas_hits", "%llu", (unsigned long long)slab_stats.cas_hits);
    APPEND_STAT("cas_badval", "%llu", (unsigned long long)slab_stats.cas_badval);
    APPEND_STAT("touch_hits", "%llu", (unsigned long long)slab_stats.touch_hits);
    APPEND_STAT("touch_misses", "%llu", (unsigned long long)thread_stats.touch_misses);
    APPEND_STAT("auth_cmds", "%llu", (unsigned long long)thread_stats.auth_cmds);
    APPEND_STAT("auth_errors", "%llu", (unsigned long long)thread_stats.auth_errors);
    APPEND_STAT("bytes_read", "%llu", (unsigned long long)thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%llu", (unsigned long long)thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("accepting_conns", "%u", stats.accepting_conns);
    APPEND_STAT("listen_disabled_num", "%llu", (unsigned long long)stats.listen_disabled_num);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%llu", (unsigned long long)thread_stats.conn_yields);
    APPEND_STAT("hash_power_level", "%u", stats.hash_power_level);
    APPEND_STAT("hash_bytes", "%llu", (unsigned long long)stats.hash_bytes);
    APPEND_STAT("hash_is_expanding", "%u", stats.hash_is_expanding);
    if (settings.slab_reassign) {
        APPEND_STAT("slab_reassign_running", "%u", stats.slab_reassign_running);
        APPEND_STAT("slabs_moved", "%llu", stats.slabs_moved);
    }
    if (settings.lru_crawler) {
        APPEND_STAT("lru_crawler_running", "%u", stats.lru_crawler_running);
    }
    APPEND_STAT("malloc_fails", "%llu",
                (unsigned long long)stats.malloc_fails);
    STATS_UNLOCK();
}

static void process_stat_settings(ADD_STAT add_stats, void *c) {
    assert(add_stats);
    APPEND_STAT("maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("udpport", "%d", settings.udpport);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_STAT("umask", "%o", settings.access);
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("num_threads_per_udp", "%d", settings.num_threads_per_udp);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
    APPEND_STAT("auth_enabled_sasl", "%s", settings.sasl ? "yes" : "no");
    APPEND_STAT("item_size_max", "%d", settings.item_size_max);
    APPEND_STAT("maxconns_fast", "%s", settings.maxconns_fast ? "yes" : "no");
    APPEND_STAT("hashpower_init", "%d", settings.hashpower_init);
    APPEND_STAT("slab_reassign", "%s", settings.slab_reassign ? "yes" : "no");
    APPEND_STAT("slab_automove", "%d", settings.slab_automove);
    APPEND_STAT("lru_crawler", "%s", settings.lru_crawler ? "yes" : "no");
    APPEND_STAT("lru_crawler_sleep", "%d", settings.lru_crawler_sleep);
    APPEND_STAT("lru_crawler_tocrawl", "%lu", (unsigned long)settings.lru_crawler_tocrawl);
    APPEND_STAT("tail_repair_time", "%d", settings.tail_repair_time);
    APPEND_STAT("flush_enabled", "%s", settings.flush_enabled ? "yes" : "no");
    APPEND_STAT("hash_algorithm", "%s", settings.hash_algorithm);
}

static void conn_to_str(const conn *c, char *buf) {
    char addr_text[MAXPATHLEN];

    if (!c) {
        strcpy(buf, "<null>");
    } else if (c->state == conn_closed) {
        strcpy(buf, "<closed>");
    } else {
        const char *protoname = "?";
        struct sockaddr_in6 local_addr;
        struct sockaddr *addr = (void *)&c->request_addr;
        int af;
        unsigned short port = 0;

        /* For listen ports and idle UDP ports, show listen address */
        if (c->state == conn_listening ||
                (IS_UDP(c->transport) &&
                 c->state == conn_read)) {
            socklen_t local_addr_len = sizeof(local_addr);

            if (getsockname(c->sfd,
                        (struct sockaddr *)&local_addr,
                        &local_addr_len) == 0) {
                addr = (struct sockaddr *)&local_addr;
            }
        }

        af = addr->sa_family;
        addr_text[0] = '\0';

        switch (af) {
            case AF_INET:
                (void) inet_ntop(af,
                        &((struct sockaddr_in *)addr)->sin_addr,
                        addr_text,
                        sizeof(addr_text) - 1);
                port = ntohs(((struct sockaddr_in *)addr)->sin_port);
                protoname = IS_UDP(c->transport) ? "udp" : "tcp";
                break;

            case AF_INET6:
                addr_text[0] = '[';
                addr_text[1] = '\0';
                if (inet_ntop(af,
                        &((struct sockaddr_in6 *)addr)->sin6_addr,
                        addr_text + 1,
                        sizeof(addr_text) - 2)) {
                    strcat(addr_text, "]");
                }
                port = ntohs(((struct sockaddr_in6 *)addr)->sin6_port);
                protoname = IS_UDP(c->transport) ? "udp6" : "tcp6";
                break;

            case AF_UNIX:
                strncpy(addr_text,
                        ((struct sockaddr_un *)addr)->sun_path,
                        sizeof(addr_text) - 1);
                addr_text[sizeof(addr_text)-1] = '\0';
                protoname = "unix";
                break;
        }

        if (strlen(addr_text) < 2) {
            /* Most likely this is a connected UNIX-domain client which
             * has no peer socket address, but there's no portable way
             * to tell for sure.
             */
            sprintf(addr_text, "<AF %d>", af);
        }

        if (port) {
            sprintf(buf, "%s:%s:%u", protoname, addr_text, port);
        } else {
            sprintf(buf, "%s:%s", protoname, addr_text);
        }
    }
}

static void process_stats_conns(ADD_STAT add_stats, void *c) {
    int i;
    char key_str[STAT_KEY_LEN];
    char val_str[STAT_VAL_LEN];
    char conn_name[MAXPATHLEN + sizeof("unix:")];
    int klen = 0, vlen = 0;

    assert(add_stats);

    for (i = 0; i < max_fds; i++) {
        if (conns[i]) {
            /* This is safe to do unlocked because conns are never freed; the
             * worst that'll happen will be a minor inconsistency in the
             * output -- not worth the complexity of the locking that'd be
             * required to prevent it.
             */
            if (conns[i]->state != conn_closed) {
                conn_to_str(conns[i], conn_name);

                APPEND_NUM_STAT(i, "addr", "%s", conn_name);
                APPEND_NUM_STAT(i, "state", "%s",
                        state_text(conns[i]->state));
                APPEND_NUM_STAT(i, "secs_since_last_cmd", "%d",
                        current_time - conns[i]->last_cmd_time);
            }
        }
    }
}

static void process_stat(conn *c, token_t *tokens, const size_t ntokens) {
    const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    assert(c != NULL);

    if (ntokens < 2) {
        out_string(c, "CLIENT_ERROR bad command line");
        return;
    }

    if (ntokens == 2) {
        server_stats(&append_stats, c);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strcmp(subcommand, "reset") == 0) {
        stats_reset();
        out_string(c, "RESET");
        return ;
    } else if (strcmp(subcommand, "detail") == 0) {
        /* NOTE: how to tackle detail with binary? */
        if (ntokens < 4)
            process_stats_detail(c, "");  /* outputs the error message */
        else
            process_stats_detail(c, tokens[2].value);
        /* Output already generated */
        return ;
    } else if (strcmp(subcommand, "settings") == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strcmp(subcommand, "cachedump") == 0) {
        char *buf;
        unsigned int bytes, id, limit = 0;

        if (ntokens < 5) {
            out_string(c, "CLIENT_ERROR bad command line");
            return;
        }

        if (!safe_strtoul(tokens[2].value, &id) ||
            !safe_strtoul(tokens[3].value, &limit)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (id >= POWER_LARGEST) {
            out_string(c, "CLIENT_ERROR Illegal slab id");
            return;
        }

        buf = item_cachedump(id, limit, &bytes);
        write_and_free(c, buf, bytes);
        return ;
    } else if (strcmp(subcommand, "conns") == 0) {
        process_stats_conns(&append_stats, c);
    } else {
        /* getting here means that the subcommand is either engine specific or
           is invalid. query the engine and see. */
        if (get_stats(subcommand, strlen(subcommand), &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                out_of_memory(c, "SERVER_ERROR out of memory writing stats");
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            // FIXME: remove the debug output
            out_string(c, "ERROR");
        }
        return ;
    }

    /* append terminator and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);

    if (c->stats.buffer == NULL) {
        out_of_memory(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

/* ntokens is overwritten here... shrug.. */
static inline void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas) {
    char *key;
    size_t nkey;
    int i = 0;
    item *it;
    token_t *key_token = &tokens[KEY_TOKEN];
    char *suffix;
    assert(c != NULL);

    do {
        while(key_token->length != 0) {

            key = key_token->value;
            nkey = key_token->length;

            if(nkey > KEY_MAX_LENGTH) {
                out_string(c, "CLIENT_ERROR bad command line format");
                while (i-- > 0) {
                    item_remove(*(c->ilist + i));
                }
                return;
            }
            // TODO: add gid/lid checking
            if (!is_my_sharding(key, nkey)) {
              //out_string(c, "CLIENT_ERROR wrong gid and lid");
              conn_set_state(c, conn_closing);
              return;
            }

            it = item_get(key, nkey);
            if (settings.detail_enabled) {
                stats_prefix_record_get(key, nkey, NULL != it);
            }
            if (it) {
                if (i >= c->isize) {
                    item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
                    if (new_list) {
                        c->isize *= 2;
                        c->ilist = new_list;
                    } else {
                        STATS_LOCK();
                        stats.malloc_fails++;
                        STATS_UNLOCK();
                        item_remove(it);
                        break;
                    }
                }

                /*
                 * Construct the response. Each hit adds three elements to the
                 * outgoing data list:
                 *   "VALUE "
                 *   key
                 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
                 */

                if (c->vnbytes < it->nbytes || (c->vnbytes >> 1) > it->nbytes) {
                    if (c->vbuf) free(c->vbuf);
                    c->vbuf = malloc(it->nbytes);
                    c->vnbytes = it->nbytes;
                }
                memcpy(c->vbuf, ecmem_get(&ecmem, it->addr), it->nbytes);

                if (return_cas)
                {
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  /* Goofy mid-flight realloc. */
                  if (i >= c->suffixsize) {
                    char **new_suffix_list = realloc(c->suffixlist,
                                           sizeof(char *) * c->suffixsize * 2);
                    if (new_suffix_list) {
                        c->suffixsize *= 2;
                        c->suffixlist  = new_suffix_list;
                    } else {
                        STATS_LOCK();
                        stats.malloc_fails++;
                        STATS_UNLOCK();
                        item_remove(it);
                        break;
                    }
                  }

                  suffix = cache_alloc(c->thread->suffix_cache);
                  if (suffix == NULL) {
                      STATS_LOCK();
                      stats.malloc_fails++;
                      STATS_UNLOCK();
                      out_of_memory(c, "SERVER_ERROR out of memory making CAS suffix");
                      item_remove(it);
                      while (i-- > 0) {
                          item_remove(*(c->ilist + i));
                      }
                      return;
                  }
                  *(c->suffixlist + i) = suffix;
                  int suffix_len = snprintf(suffix, SUFFIX_SIZE,
                                            " %llu\r\n",
                                            (unsigned long long)ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                      add_iov(c, ITEM_suffix(it), it->nsuffix - 2) != 0 ||
                      add_iov(c, suffix, suffix_len) != 0 ||
#ifdef ENABLE_COCYTUS
                      add_iov(c, c->vbuf, it->nbytes) != 0
#else
                      add_iov(c, ITEM_data(it), it->nbytes) != 0
#endif
                      )
                      {
                          item_remove(it);
                          break;
                      }
                }
                else
                {
                    // FIXME: MAYBE add_iov won't copy the real data immediately
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
#ifdef ENABLE_COCYTUS
                      add_iov(c, ITEM_suffix(it), it->nsuffix) != 0 ||
                      add_iov(c, c->vbuf, it->nbytes) != 0
#else
                      add_iov(c, ITEM_suffix(it), it->nsuffix + it->nbytes) != 0
#endif
                    )
                      {
                          item_remove(it);
                          break;
                      }
                }


                if (settings.verbose > 1) {
                    int ii;
                    fprintf(stderr, ">%d sending key ", c->sfd);
                    for (ii = 0; ii < it->nkey; ++ii) {
                        fprintf(stderr, "%c", key[ii]);
                    }
                    fprintf(stderr, "\n");
                }

                /* item_get() has incremented it->refcount for us */
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
                c->thread->stats.get_cmds++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                item_update(it);
                *(c->ilist + i) = it;
                i++;

            } else {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.get_misses++;
                c->thread->stats.get_cmds++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
            }

            key_token++;
        }

        /*
         * If the command string hasn't been fully processed, get the next set
         * of tokens.
         */
        if(key_token->value != NULL) {
            ntokens = tokenize_command(key_token->value, tokens, MAX_TOKENS);
            key_token = tokens;
        }

    } while(key_token->value != NULL);

    c->icurr = c->ilist;
    c->ileft = i;
    if (return_cas) {
        c->suffixcurr = c->suffixlist;
        c->suffixleft = i;
    }

    if (settings.verbose > 1)
        fprintf(stderr, ">%d END\n", c->sfd);

    /*
        If the loop was terminated because of out-of-memory, it is not
        reliable to add END\r\n to the buffer, because it might not end
        in \r\n. So we send SERVER_ERROR instead.
    */
    if (key_token->value != NULL || add_iov(c, "END\r\n", 5) != 0
        || (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
        out_of_memory(c, "SERVER_ERROR out of memory writing get response");
    }
    else {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    }
}

static void process_update_command(conn *c, token_t *tokens, const size_t ntokens, int comm, bool handle_cas) {
    char *key;
    size_t nkey;
    unsigned int flags;
    int32_t exptime_int = 0;
    time_t exptime;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;


    if (!is_my_sharding(key, nkey)) {
        //out_string(c, "CLIENT_ERROR wrong gid and lid");
        conn_set_state(c, conn_closing);
        return;
    }
    int lid = sub_as_lid;
    c->lid = lid;
    if (IS_PARITY_SERVER)
        switch_hashtable(lid);

    if (! (safe_strtoul(tokens[2].value, (uint32_t *)&flags)
           && safe_strtol(tokens[3].value, &exptime_int)
           && safe_strtol(tokens[4].value, (int32_t *)&vlen))) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
    exptime = exptime_int;

    /* Negative exptimes can underflow and end up immortal. realtime() will
       immediately expire values that are greater than REALTIME_MAXDELTA, but less
       than process_started, so lets aim for that. */
    if (exptime < 0)
        exptime = REALTIME_MAXDELTA + 1;

    // does cas value exist?
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    vlen += 2;
    if (vlen < 0 || vlen - 2 < 0) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, flags, realtime(exptime), vlen);

    if (it == 0) {
        if (! item_size_ok(nkey, flags, vlen))
            out_string(c, "SERVER_ERROR object too large for cache");
        else
            out_of_memory(c, "SERVER_ERROR out of memory storing object");
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (comm == NREAD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        return;
    }
    ITEM_set_cas(it, req_cas_id);

    c->item = it;
#ifdef ENABLE_COCYTUS
    if (c->vnbytes < it->nbytes || (c->vnbytes >> 1) > it->nbytes) {
        if (c->vbuf) free(c->vbuf);
        c->vbuf = malloc(it->nbytes);
        c->vnbytes = it->nbytes;
    }
    c->ritem = c->vbuf;
#else
    c->ritem = ITEM_data(it);
#endif
    c->rlbytes = it->nbytes;
    c->cmd = comm;
    conn_set_state(c, conn_nread);
}

static void process_touch_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;
    int32_t exptime_int = 0;
    item *it;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtol(tokens[2].value, &exptime_int)) {
        out_string(c, "CLIENT_ERROR invalid exptime argument");
        return;
    }

    it = item_touch(key, nkey, realtime(exptime_int));
    if (it) {
        item_update(it);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "TOUCHED");
        item_remove(it);
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.touch_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

static void process_arithmetic_command(conn *c, token_t *tokens, const size_t ntokens, const bool incr) {
    char temp[INCR_MAX_STORAGE_LEN];
    uint64_t delta;
    char *key;
    size_t nkey;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtoull(tokens[2].value, &delta)) {
        out_string(c, "CLIENT_ERROR invalid numeric delta argument");
        return;
    }

    switch(add_delta(c, key, nkey, incr, delta, temp, NULL)) {
    case OK:
        out_string(c, temp);
        break;
    case NON_NUMERIC:
        out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        break;
    case EOM:
        out_of_memory(c, "SERVER_ERROR out of memory");
        break;
    case DELTA_ITEM_NOT_FOUND:
        pthread_mutex_lock(&c->thread->stats.mutex);
        if (incr) {
            c->thread->stats.incr_misses++;
        } else {
            c->thread->stats.decr_misses++;
        }
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
        break;
    case DELTA_ITEM_CAS_MISMATCH:
        break; /* Should never get here */
    }
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
enum delta_result_type do_add_delta(conn *c, const char *key, const size_t nkey,
                                    const bool incr, const int64_t delta,
                                    char *buf, uint64_t *cas,
                                    const uint32_t hv) {
    char *ptr;
    uint64_t value;
    int res;
    item *it;

    it = do_item_get(key, nkey, hv);
    if (!it) {
        return DELTA_ITEM_NOT_FOUND;
    }

    if (cas != NULL && *cas != 0 && ITEM_get_cas(it) != *cas) {
        do_item_remove(it);
        return DELTA_ITEM_CAS_MISMATCH;
    }

    ptr = ITEM_data(it);

    if (!safe_strtoull(ptr, &value)) {
        do_item_remove(it);
        return NON_NUMERIC;
    }

    if (incr) {
        value += delta;
        MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
    } else {
        if(delta > value) {
            value = 0;
        } else {
            value -= delta;
        }
        MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
    }

    pthread_mutex_lock(&c->thread->stats.mutex);
    if (incr) {
        c->thread->stats.slab_stats[it->slabs_clsid].incr_hits++;
    } else {
        c->thread->stats.slab_stats[it->slabs_clsid].decr_hits++;
    }
    pthread_mutex_unlock(&c->thread->stats.mutex);

    snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)value);
    res = strlen(buf);
    /* refcount == 2 means we are the only ones holding the item, and it is
     * linked. We hold the item's lock in this function, so refcount cannot
     * increase. */
    if (res + 2 <= it->nbytes && it->refcount == 2) { /* replace in-place */
        /* When changing the value without replacing the item, we
           need to update the CAS on the existing item. */
        mutex_lock(&cache_lock); /* FIXME */
        ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
        mutex_unlock(&cache_lock);

        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
        do_item_update(it);
    } else if (it->refcount > 1) {
        item *new_it;
        new_it = do_item_alloc(ITEM_key(it), it->nkey, atoi(ITEM_suffix(it) + 1), it->exptime, res + 2, hv);
        if (new_it == 0) {
            do_item_remove(it);
            return EOM;
        }
        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        item_replace(it, new_it, hv);
        // Overwrite the older item's CAS with our new CAS since we're
        // returning the CAS of the old item below.
        ITEM_set_cas(it, (settings.use_cas) ? ITEM_get_cas(new_it) : 0);
        do_item_remove(new_it);       /* release our reference */
    } else {
        /* Should never get here. This means we somehow fetched an unlinked
         * item. TODO: Add a counter? */
        if (settings.verbose) {
            fprintf(stderr, "Tried to do incr/decr on invalid item\n");
        }
        if (it->refcount == 1)
            do_item_remove(it);
        return DELTA_ITEM_NOT_FOUND;
    }

    if (cas) {
        *cas = ITEM_get_cas(it);    /* swap the incoming CAS value */
    }
    do_item_remove(it);         /* release our reference */
    return OK;
}

static void process_delete_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;
    item *it;

    assert(c != NULL);

    if (ntokens > 3) {
        bool hold_is_zero = strcmp(tokens[KEY_TOKEN+1].value, "0") == 0;
        bool sets_noreply = set_noreply_maybe(c, tokens, ntokens);
        bool valid = (ntokens == 4 && (hold_is_zero || sets_noreply))
            || (ntokens == 5 && hold_is_zero && sets_noreply);
        if (!valid) {
            out_string(c, "CLIENT_ERROR bad command line format.  "
                       "Usage: delete <key> [noreply]");
            return;
        }
    }


    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if(nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        item_unlink(it);
        item_remove(it);      /* release our reference */
        out_string(c, "DELETED");
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

static void process_verbosity_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    level = strtoul(tokens[1].value, NULL, 10);
    settings.verbose = level > MAX_VERBOSITY_LEVEL ? MAX_VERBOSITY_LEVEL : level;
    out_string(c, "OK");
    return;
}

static void process_slabs_automove_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    level = strtoul(tokens[2].value, NULL, 10);
    if (level == 0) {
        settings.slab_automove = 0;
    } else if (level == 1 || level == 2) {
        settings.slab_automove = level;
    } else {
        out_string(c, "ERROR");
        return;
    }
    out_string(c, "OK");
    return;
}


static void process_command(conn *c, char *command) {

    token_t tokens[MAX_TOKENS];
    size_t ntokens;
    int comm;

    assert(c != NULL);

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d %s\n", c->sfd, command);

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        out_of_memory(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

    ntokens = tokenize_command(command, tokens, MAX_TOKENS);

    if (ntokens >= 3 &&
        ((strcmp(tokens[COMMAND_TOKEN].value, "get") == 0) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0))) {

        c->start_w_rep = w_rep_cnt;
        c->start_w_req = w_req_cnt;
        c->start_r_rep = r_rep_cnt;
        c->start_r_req = r_req_cnt;
        r_req_cnt++;
        c->is_write = 0;

#ifdef ENABLE_COCYTUS
        if (IS_PARITY_SERVER) {
            char *key = tokens[KEY_TOKEN].value;
            int nkey = tokens[KEY_TOKEN].length;

            if(nkey > KEY_MAX_LENGTH) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            if (!is_my_sharding(key, nkey)) {
              //out_string(c, "CLIENT_ERROR wrong gid and lid");
              conn_set_state(c, conn_closing);
              return;
            }
            int lid = sub_as_lid;
            switch_hashtable(lid);

            item *it = item_get(key, nkey);
            // TODO
            if (!it) {
                out_string(c, "END");
                return;
            }
            assert(it->refcount > 1);
            uint64_t addr = it->addr;
            uint64_t size = it->nbytes;

            int ubegin = recovery_calc_unit_id(&recovery, addr);
            int uend = recovery_calc_unit_id(&recovery, addr + size - 1);

            c->item = it;

            //try_do_recovery(c, ubegin, uend, conn_recovery_complete_for_get);
            //int nwait = try_do_recovery(NULL, ubegin, uend, conn_recovery_complete_for_get);
            try_do_recovery(c, ubegin, uend, conn_recovery_complete_for_get);

	    //if (nwait)
	    //	    conn_set_state(c, conn_closing);
	    //else
	    //	    conn_set_state(c, conn_recovery_complete_for_get);


        } else
#endif
        process_get_command(c, tokens, ntokens, false);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strcmp(tokens[COMMAND_TOKEN].value, "add") == 0 && (comm = NREAD_ADD)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "set") == 0 && (comm = NREAD_SET)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0 && (comm = NREAD_REPLACE)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0 && (comm = NREAD_PREPEND)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "append") == 0 && (comm = NREAD_APPEND)) )) {

        c->start_w_rep = w_rep_cnt;
        c->start_w_req = w_req_cnt;
        c->start_r_rep = r_rep_cnt;
        c->start_r_req = r_req_cnt;
        w_req_cnt++;
        c->is_write = 1;
        process_update_command(c, tokens, ntokens, comm, false);

#ifdef ENABLE_COCYTUS
    } else if ((ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "subpeer") == 0)) {
        // subpeer $gid $leaderlid $deadlid

        int gid, leaderlid, deadlid;
        safe_strtol(tokens[1].value, (int *)&gid);
        safe_strtol(tokens[2].value, (int *)&leaderlid);
        safe_strtol(tokens[3].value, (int *)&deadlid);
        debug("receive subpeer gid=%d leader=%d dead=%d\n", gid, leaderlid, deadlid);

        uint64_t maxxid = rep_queue_maxxid(peers[deadlid].rep_queue, peers[deadlid].done_xid);

        // subpeerack $gid $lid $maxlid
        send_msgf(peers[leaderlid].conn_w, "subpeerack %d %d %"PRIu64,
                settings.gid, settings.lid, maxxid);

        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "subpeerack") == 0)) {
        // precondition: no other failure will occur during the subpeer-subpeerack process
        // statistic the smallest one
        int gid, lid;
        uint64_t xid;
        safe_strtol(tokens[1].value, (int *)&gid);
        safe_strtol(tokens[2].value, (int *)&lid);
        safe_strtoull(tokens[3].value, &xid);
        if (xid < stable_xid) stable_xid = xid;
        stable_nxid--;

        if (stable_nxid == 0) {
            alloc_xid = stable_xid + 1;
            sub_as_lid = -sub_as_lid;
            sub_as_ready = 1;

            process_queued_items(sub_as_lid, stable_xid);

            int i;

            int ucount = MEMSIZE / UNITSIZE;
            sub_init_need_recovery_count = 0;
            sub_finished_recovery_count = 0;
            end_unit = 0;
            for (i=0; i<ucount; ++i) {
                sub_flags[i] = peers[sub_as_lid].touch_flags[i] ? 0 : 2;
                if (sub_flags[i] == 0) end_unit = i+1;
                sub_init_need_recovery_count += peers[sub_as_lid].touch_flags[i];
            }
            update_idle_event(idle_recovery_event, EV_WRITE | EV_PERSIST);

            for (i=settings.nshard; i<settings.nparity+settings.nshard; ++i) {
                if (i == settings.lid) continue;
                if (peers[i].state == peer_connected) {
                    // subpeerackack $gid $leaderlid $deadlid $maxxid
                    send_msgf(peers[i].conn_w, "subpeerackack %d %d %d %"PRIu64,
                            settings.gid, settings.lid, sub_as_lid, stable_xid);
                }

            }

        }
        conn_set_state(c, conn_new_cmd);
    } else if ((ntokens == 6) && (strcmp(tokens[COMMAND_TOKEN].value, "subpeerackack") == 0)) {
        // subpeerackack $gid $leaderlid $deadlid $maxxid
        int gid, leaderlid, deadlid;
        uint64_t maxxid;
        safe_strtol(tokens[1].value, (int *)&gid);
        safe_strtol(tokens[2].value, (int *)&leaderlid);
        safe_strtol(tokens[3].value, (int *)&deadlid);
        safe_strtoull(tokens[4].value, &maxxid);

        process_queued_items(deadlid, maxxid);

        // FIXME: consider parity fail
        if (IS_PARITY_SERVER) {
            if (recovery_leader == settings.lid) {
                restart_failed_recovery(deadlid);
            }
        }

        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_req") == 0)) {

        int req_lid, ubegin, uend;
        conn *conn_ptr = NULL;
        req_lid = get_real_peer_lid_by_conn(c); // req_lid
        safe_strtol(tokens[1].value, (int *)&ubegin);
        safe_strtol(tokens[2].value, (int *)&uend);
        safe_strtoull(tokens[3].value, (uint64_t *)&conn_ptr);

        int i = ubegin;
        while (i<=uend) {
            while (i<=uend && sub_flags[i] == 1) {
                // those = 1 don't need to call recovery
                i++;
            }
            if (i>uend) break;
            int start = i;
            int startflag = sub_flags[start];
            while (i<=uend && sub_flags[i] == startflag) {
                // those = 0 or 2 need to recover
                if (sub_flags[i] == 0) sub_flags[i] = 1;
                i++;
            }

            if (startflag == 0) {
                start_recovery(start, i-1, req_lid, conn_ptr);
            } else {
                start_fast_recovery(start, i-1, req_lid, conn_ptr);
            }
        }

        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 6) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_units_scatter") == 0)) {
        // recover_units_scatter $unit_begin $unit_end $mask
        // data...
        int lid, unit_begin, unit_end;
        uint32_t mask;
        conn *conn_ptr = NULL;
        lid = get_real_peer_lid_by_conn(c); // leader
        safe_strtol(tokens[1].value, (int *)&unit_begin);
        safe_strtol(tokens[2].value, (int *)&unit_end);
        safe_strtoul(tokens[3].value, &mask);
        safe_strtoull(tokens[4].value, (uint64_t *)&conn_ptr);

        c->mask = mask;

        c->rqit = recovery_req_find(&recovery, lid, unit_begin, unit_end, mask);
        struct recovery_queue_item *item = c->rqit;
        // TODO: MORE CHECKS
        item->conn = conn_ptr;
        debug("item->conn %p \n", (void *)item->conn);
        //assert(item->mask & (1<<lid));

        if (c->vbuf) {
            free(c->vbuf);
            c->vbuf = NULL;
            c->vnbytes = 0;
        }
        c->rlbytes = c->vnbytes = recovery_calc_total_length(&recovery, unit_begin, unit_end);
        c->ritem = c->vbuf = malloc(c->vnbytes);


        conn_set_state(c, conn_recovery_scatter_nread);

    } else if ((ntokens == 7) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_units_gather") == 0)) {
        // recover_units_gather $leader_lid $lid $unit_begin $unit_end $mask
        // data...
        int leader_lid, lid, unit_begin, unit_end;
        uint32_t mask;
        safe_strtol(tokens[1].value, (int *)&leader_lid);
        assert(leader_lid == settings.lid);
        safe_strtol(tokens[2].value, (int *)&lid);
        safe_strtol(tokens[3].value, (int *)&unit_begin);
        safe_strtol(tokens[4].value, (int *)&unit_end);
        safe_strtoul(tokens[5].value, &mask);

        c->rqit = recovery_req_find(&recovery, leader_lid, unit_begin, unit_end, mask);
        struct recovery_queue_item *item = c->rqit;
        if (c->vbuf) {
            free(c->vbuf); c->vbuf = NULL; c->vnbytes = 0;
        }
        if (item) {
            assert(item->mask & (1<<lid));
            assert((item->flags & (1<<lid)) == 0);
            assert(item->data_from_parity);
            assert(item->data_from_parity[lid] == NULL);
            c->rlbytes = c->vnbytes = recovery_calc_total_length(&recovery, unit_begin, unit_end);
            c->ritem = c->vbuf = item->data_from_parity[lid] = malloc(c->vnbytes);
        } else {
            c->rlbytes = c->vnbytes = recovery_calc_total_length(&recovery, unit_begin, unit_end);
            c->ritem = c->vbuf = malloc(c->vnbytes);
        }

        conn_set_state(c, conn_recovery_gather_nread);

    } else if ((ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "pre_alloc") == 0)) {
        int lid = get_peer_lid_by_conn(c);
        int nbytes;
        uint64_t addr;
        uint64_t got_stable_xid;
        safe_strtoull(tokens[1].value, (uint64_t *)&addr);
        safe_strtol(tokens[2].value, (int *)&nbytes);
        safe_strtoull(tokens[3].value, (uint64_t *)&got_stable_xid);

        switch_hashtable(lid);
        while (peers[lid].done_xid < got_stable_xid) {
            process_rep_command(c, ++peers[lid].done_xid, -1);
        }

        debug("pre_alloc %"PRIu64" %d for %d\n", addr, nbytes, lid);
        uint64_t myaddr = ec_alloc(&ecallocs[lid], nbytes);
        debug(BG_G"ec_alloc get %"PRIu64" in pre_alloc"BG_D"\n", myaddr);
        myaddr = myaddr;
        assert(get_real_peer_lid_by_conn(c) != lid);
        assert(addr == myaddr);

        pac_queue_add(get_real_peer_lid_by_conn(c), myaddr);

        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 6) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_units") == 0)) {
        // recover_unit $leader_lid $unit_begin $unit_end $mask
        // NOTICE: mask contains all peer bits involved in this recovery
        // [begin,end]
        // FIXME : The unit of different request may overlap, so there are
        // dependencies among requests which should be noticed and handled
        // gracefully.

        int leader_lid, unit_begin, unit_end;
        uint32_t mask;
        safe_strtol(tokens[1].value, (int *)&leader_lid);
        safe_strtol(tokens[2].value, (int *)&unit_begin);
        safe_strtol(tokens[3].value, (int *)&unit_end);
        safe_strtoul(tokens[4].value, (uint32_t *)&mask);
        //assert(mask & (1<<leader_lid));
        assert(mask & (1<<settings.lid));
        assert( ! IS_PARITY_SERVER );
        int i;
        uint64_t addr = recovery_calc_start_addr(&recovery, unit_begin);
        uint32_t len = recovery_calc_total_length(
                &recovery, unit_begin, unit_end);
        debug("recover_len: %d\n", (int)len);


        FOREACH_PARITY_LID(i) {

            if ((mask & (1<<i)) == 0) continue;

            // retrieve the local data and send it back
            // all xid < $maxxid should be processed before applying this patch
            // sendback:
            // recover_unit_reply $leader_lid $unit_begin $unit_end $mask $maxxid
            // realdata of length len
            send_msgf_raw(peers[i].conn_w,
                    "recover_units_reply %d %d %d %"PRIu32" %"PRIu64,
                    leader_lid, unit_begin, unit_end, mask, stable_xid);
            send_msgbuf_raw(peers[i].conn_w, ecmem_get(&ecmem, addr), len);

            if (peers[i].state == peer_connected) {
                conn_set_state(peers[i].conn_w, conn_write_to_peer);
                event_active(&peers[i].conn_w->event, EV_WRITE, 0);
            }

        }
        conn_set_state(c, conn_new_cmd);

        //
    } else if ((ntokens == 7) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_units_reply") == 0)) {
        // receive first line of the reply
        // thus: recover_units_reply $leader_lid $unit_begin $unit_end $mask $maxxid
        // and turn to conn_nread
        //int lid = get_peer_lid_by_conn(c);
        int leader_lid, unit_begin, unit_end;
        uint32_t mask;
        uint64_t got_stable_xid;
        safe_strtol(tokens[1].value, (int *)&leader_lid);
        safe_strtol(tokens[2].value, (int *)&unit_begin);
        safe_strtol(tokens[3].value, (int *)&unit_end);
        safe_strtoul(tokens[4].value, &mask);
        safe_strtoull(tokens[5].value, &got_stable_xid);

        if (sub_as_lid < 0) {
            printf("sub-as-lid %d\n", sub_as_lid);
            exit(-1);
        }

        if (c->vbuf) {
            free(c->vbuf); c->vbuf = NULL; c->vnbytes = 0;
        }
        // TODO: realloc vbuf
        c->rlbytes = c->vnbytes = recovery_calc_total_length(&recovery, unit_begin, unit_end);
        c->ritem = c->vbuf = malloc(c->vnbytes);

        int lid = get_peer_lid_by_conn(c);
        switch_hashtable(lid);

        while (peers[lid].done_xid < got_stable_xid) {
            process_rep_command(c, ++peers[lid].done_xid, -1);
        }
        // not used
        c->lid = 0;
        c->mask = 0;

        struct recovery_queue_item *rqit = recovery_req_find(&recovery, leader_lid, unit_begin, unit_end, mask);
        if (rqit == NULL) {
            // add request to recovery queue
            rqit = recovery_req_add(&recovery, -1, NULL, leader_lid, unit_begin, unit_end, mask);
        }
        c->rqit = rqit;

        conn_set_state(c, conn_recovery_nread);

    } else if ((ntokens == 11) && (strcmp(tokens[COMMAND_TOKEN].value, "recover_unit_reply_ack") == 0)) {
        // receive

    } else if ((ntokens == 12) && (strcmp(tokens[COMMAND_TOKEN].value, "rep") == 0)) {

        assert(IS_PARITY_SERVER);
        uint64_t got_stable_xid;
        safe_strtoull(tokens[10].value, (uint64_t *)&got_stable_xid);

        int lid = get_peer_lid_by_conn(c);
        switch_hashtable(lid);

        while (peers[lid].done_xid < got_stable_xid) {
            process_rep_command(c, ++peers[lid].done_xid, -1);
        }

        queue_rep_command(c, tokens, ntokens, 0, 0);

    } else if ((ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "repack") == 0)) {

        assert((!IS_PARITY_SERVER) || sub_as_lid != settings.lid);

        uint64_t xid;
        //int lid = get_peer_lid_by_conn(c);
        //assert(lid != -1);
        conn *conn_ptr;
        safe_strtoull(tokens[2].value, (uint64_t *)&conn_ptr);
        safe_strtoull(tokens[3].value, (uint64_t *)&xid);
        debug("got repack %s %p %"PRIu64 "\n", tokens[1].value, (void *)conn_ptr, xid);
        conn_ptr->xid = xid;

        int lid = get_real_peer_lid_by_conn(c);
        conn_ptr->ack &= ~(1<<lid);
        debug("==========> %x <=========\n", conn_ptr->ack);
        if (conn_ptr->ack == 0) {
            // active target
            debug("active target conn_ptr=%p\n", (void *)conn_ptr);
            //assert(conn_ptr->state == conn_waiting_ack);
            event_active(&conn_ptr->event, EV_WRITE, 0);
        }

        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 3) && (strcmp(tokens[COMMAND_TOKEN].value, "repackack") == 0)) {
        uint64_t xid;
        safe_strtoull(tokens[1].value, (uint64_t *)&xid);
        debug("repackack: received xid=%lld\n", (long long) xid);
        process_rep_command(c, xid, -1);

    } else if ((ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "peerconn") == 0)) {

        //assert(IS_PARITY_SERVER);
        //assert(peers_read_ready < settings.npeer - 2);

        int gid, lid;
        if (!safe_strtol(tokens[1].value, &gid) || !safe_strtol(tokens[2].value, &lid)) {
            printf("error parsing peerconn\n");
            exit(-1);
        }
        assert(gid == settings.gid);
        debug("======> %p    %p <=======\n", (void *)c, (void *)peers[lid].conn_r);
        assert(peers[lid].conn_r == NULL || peers[lid].conn_r == c);
        peers[lid].conn_r = c;
        if (peers[lid].conn_w == NULL) {
            cocytus_connect_to_peer(lid);
        }

        //
        //pthread_mutex_lock(&peers_ready_mutex);
        // read from lid <= mylid
        peers_read_ready++;
        debug("peerconn <- <- %d\n", lid);

        if (peers_read_ready == settings.nparity + settings.nshard - 1) {
            debug("peerconn all received\n");
            // send peerok to everyone
            int i;
            for (i=0; i<settings.nshard+settings.nparity; ++i) {
                if (i == settings.lid) continue;
                printf("sending peerok...\n");
                //pthread_mutex_lock(&peers[i].mutex);
                assert(peers[i].conn_w);
                //send_short_msg(peers[i].conn_w, "peerok", "");
                send_msgf(peers[i].conn_w, "peerok %d %d", settings.gid, settings.lid);
                event_active(&peers[i].conn_w->event, EV_WRITE, 0);
                //pthread_mutex_unlock(&peers[i].mutex);
            }
            debug("All nodes connected to me!\n");
        }
        //pthread_mutex_unlock(&peers_ready_mutex);
        // working = true;
        conn_set_state(c, conn_new_cmd);

    } else if ((ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "peerok") == 0)) {

        //assert(!IS_PARITY_SERVER);
        //pthread_mutex_lock(&peers_ready_mutex);
        peers_write_ready++;
        debug("peerok from <--- %d\n", get_peer_lid_by_conn(c));
        if (peers_write_ready == settings.nparity + settings.nshard - 1) {
            debug("All nodes are connected by me!\n");
        }
        //pthread_mutex_unlock(&peers_ready_mutex);
        conn_set_state(c, conn_new_cmd);

#endif
    } else if ((ntokens == 7 || ntokens == 8) && (strcmp(tokens[COMMAND_TOKEN].value, "cas") == 0 && (comm = NREAD_CAS))) {

        process_update_command(c, tokens, ntokens, comm, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 1);

    } else if (ntokens >= 3 && (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)) {

        process_get_command(c, tokens, ntokens, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 0);

    } else if (ntokens >= 3 && ntokens <= 5 && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0)) {

        process_delete_command(c, tokens, ntokens);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "touch") == 0)) {

        process_touch_command(c, tokens, ntokens);

    } else if (ntokens >= 2 && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0)) {

        process_stat(c, tokens, ntokens);

    } else if (ntokens >= 2 && ntokens <= 4 && (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0)) {
        time_t exptime = 0;

        set_noreply_maybe(c, tokens, ntokens);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.flush_cmds++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        if (!settings.flush_enabled) {
            // flush_all is not allowed but we log it on stats
            out_string(c, "CLIENT_ERROR flush_all not allowed");
            return;
        }

        if(ntokens == (c->noreply ? 3 : 2)) {
            settings.oldest_live = current_time - 1;
            item_flush_expired();
            out_string(c, "OK");
            return;
        }

        exptime = strtol(tokens[1].value, NULL, 10);
        if(errno == ERANGE) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        /*
          If exptime is zero realtime() would return zero too, and
          realtime(exptime) - 1 would overflow to the max unsigned
          value.  So we process exptime == 0 the same way we do when
          no delay is given at all.
        */
        if (exptime > 0)
            settings.oldest_live = realtime(exptime) - 1;
        else /* exptime == 0 */
            settings.oldest_live = current_time - 1;
        item_flush_expired();
        out_string(c, "OK");
        return;

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0)) {

        out_string(c, "VERSION " VERSION);

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0)) {

        //debug("conn quit\n");
        conn_set_state(c, conn_closing);

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "shutdown") == 0)) {

        if (settings.shutdown_command) {
            //debug("conn shutdown\n");
            conn_set_state(c, conn_closing);
            raise(SIGINT);
        } else {
            out_string(c, "ERROR: shutdown not enabled");
        }

    } else if (ntokens > 1 && strcmp(tokens[COMMAND_TOKEN].value, "slabs") == 0) {
        if (ntokens == 5 && strcmp(tokens[COMMAND_TOKEN + 1].value, "reassign") == 0) {
            int src, dst, rv;

            if (settings.slab_reassign == false) {
                out_string(c, "CLIENT_ERROR slab reassignment disabled");
                return;
            }

            src = strtol(tokens[2].value, NULL, 10);
            dst = strtol(tokens[3].value, NULL, 10);

            if (errno == ERANGE) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            rv = slabs_reassign(src, dst);
            switch (rv) {
            case REASSIGN_OK:
                out_string(c, "OK");
                break;
            case REASSIGN_RUNNING:
                out_string(c, "BUSY currently processing reassign request");
                break;
            case REASSIGN_BADCLASS:
                out_string(c, "BADCLASS invalid src or dst class id");
                break;
            case REASSIGN_NOSPARE:
                out_string(c, "NOSPARE source class has no spare pages");
                break;
            case REASSIGN_SRC_DST_SAME:
                out_string(c, "SAME src and dst class are identical");
                break;
            }
            return;
        } else if (ntokens == 4 &&
            (strcmp(tokens[COMMAND_TOKEN + 1].value, "automove") == 0)) {
            process_slabs_automove_command(c, tokens, ntokens);
        } else {
            out_string(c, "ERROR");
        }
    } else if (ntokens > 1 && strcmp(tokens[COMMAND_TOKEN].value, "lru_crawler") == 0) {
        if (ntokens == 4 && strcmp(tokens[COMMAND_TOKEN + 1].value, "crawl") == 0) {
            int rv;
            if (settings.lru_crawler == false) {
                out_string(c, "CLIENT_ERROR lru crawler disabled");
                return;
            }

            rv = lru_crawler_crawl(tokens[2].value);
            switch(rv) {
            case CRAWLER_OK:
                out_string(c, "OK");
                break;
            case CRAWLER_RUNNING:
                out_string(c, "BUSY currently processing crawler request");
                break;
            case CRAWLER_BADCLASS:
                out_string(c, "BADCLASS invalid class id");
                break;
            }
            return;
        } else if (ntokens == 4 && strcmp(tokens[COMMAND_TOKEN + 1].value, "tocrawl") == 0) {
            uint32_t tocrawl;
             if (!safe_strtoul(tokens[2].value, &tocrawl)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            settings.lru_crawler_tocrawl = tocrawl;
            out_string(c, "OK");
            return;
        } else if (ntokens == 4 && strcmp(tokens[COMMAND_TOKEN + 1].value, "sleep") == 0) {
            uint32_t tosleep;
            if (!safe_strtoul(tokens[2].value, &tosleep)) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }
            if (tosleep > 1000000) {
                out_string(c, "CLIENT_ERROR sleep must be one second or less");
                return;
            }
            settings.lru_crawler_sleep = tosleep;
            out_string(c, "OK");
            return;
        } else if (ntokens == 3) {
            if ((strcmp(tokens[COMMAND_TOKEN + 1].value, "enable") == 0)) {
                if (start_item_crawler_thread() == 0) {
                    out_string(c, "OK");
                } else {
                    out_string(c, "ERROR failed to start lru crawler thread");
                }
            } else if ((strcmp(tokens[COMMAND_TOKEN + 1].value, "disable") == 0)) {
                if (stop_item_crawler_thread() == 0) {
                    out_string(c, "OK");
                } else {
                    out_string(c, "ERROR failed to stop lru crawler thread");
                }
            } else {
                out_string(c, "ERROR");
            }
            return;
        } else {
            out_string(c, "ERROR");
        }
    } else if ((ntokens == 3 || ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "verbosity") == 0)) {
        process_verbosity_command(c, tokens, ntokens);
    } else {
        // FIXME : maybe all the out_string of peer connections should be protected by locks
        debug("============> final else <============\n");
        // THIS MAY WRITE TO A PEER CONNECTION THAT MAY HAVE DATA RACE
        out_string(c, "ERROR");
    }
    return;
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(conn *c) {
    debug("try read command\n");
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);
#ifdef ENABLE_COCYTUS
    if (!IS_PARITY_SERVER) {
        switch_hashtable(0);
    }
#endif

    if (c->protocol == negotiating_prot || c->transport == udp_transport)  {
        if ((unsigned char)c->rbuf[0] == (unsigned char)PROTOCOL_BINARY_REQ) {
            c->protocol = binary_prot;
        } else {
            c->protocol = ascii_prot;
        }

        if (settings.verbose > 1) {
            fprintf(stderr, "%d: Client using the %s protocol\n", c->sfd,
                    prot_text(c->protocol));
        }
    }

    if (c->protocol == binary_prot) {
        /* Do we have the complete packet header? */
        if (c->rbytes < sizeof(c->binary_header)) {
            /* need more data! */
            return 0;
        } else {
#ifdef NEED_ALIGN
            if (((long)(c->rcurr)) % 8 != 0) {
                /* must realign input buffer */
                memmove(c->rbuf, c->rcurr, c->rbytes);
                c->rcurr = c->rbuf;
                if (settings.verbose > 1) {
                    fprintf(stderr, "%d: Realign input buffer\n", c->sfd);
                }
            }
#endif
            protocol_binary_request_header* req;
            req = (protocol_binary_request_header*)c->rcurr;

            if (settings.verbose > 1) {
                /* Dump the packet before we convert it to host order */
                int ii;
                fprintf(stderr, "<%d Read binary protocol data:", c->sfd);
                for (ii = 0; ii < sizeof(req->bytes); ++ii) {
                    if (ii % 4 == 0) {
                        fprintf(stderr, "\n<%d   ", c->sfd);
                    }
                    fprintf(stderr, " 0x%02x", req->bytes[ii]);
                }
                fprintf(stderr, "\n");
            }

            c->binary_header = *req;
            c->binary_header.request.keylen = ntohs(req->request.keylen);
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.cas = ntohll(req->request.cas);

            if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ) {
                if (settings.verbose) {
                    fprintf(stderr, "Invalid magic:  %x\n",
                            c->binary_header.request.magic);
                }
                conn_set_state(c, conn_closing);
                return -1;
            }

            c->msgcurr = 0;
            c->msgused = 0;
            c->iovused = 0;
            if (add_msghdr(c) != 0) {
                out_of_memory(c,
                        "SERVER_ERROR Out of memory allocating headers");
                return 0;
            }

            c->cmd = c->binary_header.request.opcode;
            c->keylen = c->binary_header.request.keylen;
            c->opaque = c->binary_header.request.opaque;
            /* clear the returned cas value */
            c->cas = 0;

            dispatch_bin_command(c);

            c->rbytes -= sizeof(c->binary_header);
            c->rcurr += sizeof(c->binary_header);
        }
    } else {
        char *el, *cont;

        if (c->rbytes == 0)
            return 0;

        el = memchr(c->rcurr, '\n', c->rbytes);
        if (!el) {
            if (c->rbytes > 1024) {
                /*
                 * We didn't have a '\n' in the first k. This _has_ to be a
                 * large multiget, if not we should just nuke the connection.
                 */
                char *ptr = c->rcurr;
                while (*ptr == ' ') { /* ignore leading whitespaces */
                    ++ptr;
                }

                if (ptr - c->rcurr > 100 ||
                    (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {

                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }

            return 0;
        }
        cont = el + 1;
        if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
            el--;
        }
        *el = '\0';

        assert(cont <= (c->rcurr + c->rbytes));

        c->last_cmd_time = current_time;
        process_command(c, c->rcurr);

        c->rbytes -= (cont - c->rcurr);
        c->rcurr = cont;

        assert(c->rcurr <= (c->rbuf + c->rsize));
    }

    return 1;
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c) {
    int res;

    assert(c != NULL);

    c->request_addr_size = sizeof(c->request_addr);
    res = recvfrom(c->sfd, c->rbuf, c->rsize,
                   0, (struct sockaddr *)&c->request_addr,
                   &c->request_addr_size);
    if (res > 8) {
        unsigned char *buf = (unsigned char *)c->rbuf;
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.bytes_read += res;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        /* Beginning of UDP packet is the request ID; save it. */
        c->request_id = buf[0] * 256 + buf[1];

        /* If this is a multi-packet request, drop it. */
        if (buf[4] != 0 || buf[5] != 1) {
            out_string(c, "SERVER_ERROR multi-packet request not supported");
            return READ_NO_DATA_RECEIVED;
        }

        /* Don't care about any of the rest of the header. */
        res -= 8;
        memmove(c->rbuf, c->rbuf + 8, res);

        c->rbytes = res;
        c->rcurr = c->rbuf;
        return READ_DATA_RECEIVED;
    }
    return READ_NO_DATA_RECEIVED;
}

/*
 * read from network as much as we can, handle buffer overflow and connection
 * close.
 * before reading, move the remaining incomplete fragment of a command
 * (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus data causing
 * the connection to eat up all available memory, break out and start looking
 * at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c) {
    enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
    int res;
    int num_allocs = 0;
    assert(c != NULL);

    if (c->rcurr != c->rbuf) {
        if (c->rbytes != 0) /* otherwise there's nothing to copy */
            memmove(c->rbuf, c->rcurr, c->rbytes);
        c->rcurr = c->rbuf;
    }

    while (1) {
        if (c->rbytes >= c->rsize) {
            if (num_allocs == 4) {
                return gotdata;
            }
            ++num_allocs;
            char *new_rbuf = realloc(c->rbuf, c->rsize * 2);
            if (!new_rbuf) {
                STATS_LOCK();
                stats.malloc_fails++;
                STATS_UNLOCK();
                if (settings.verbose > 0) {
                    fprintf(stderr, "Couldn't realloc input buffer\n");
                }
                c->rbytes = 0; /* ignore what we read */
                out_of_memory(c, "SERVER_ERROR out of memory reading request");
                c->write_and_go = conn_closing;
                return READ_MEMORY_ERROR;
            }
            c->rcurr = c->rbuf = new_rbuf;
            c->rsize *= 2;
        }

        int avail = c->rsize - c->rbytes;
        res = read(c->sfd, c->rbuf + c->rbytes, avail);
        if (res > 0) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_read += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            gotdata = READ_DATA_RECEIVED;
            c->rbytes += res;
            if (res == avail) {
                continue;
            } else {
                break;
            }
        }
        if (res == 0) {
            debug("read nothing -> error\n");
            return READ_ERROR;
        }
        if (res == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            debug("read error -> %s\n", strerror(errno));
            return READ_ERROR;
        }
    }
    return gotdata;
}

static bool update_idle_event(struct event* e, const int new_flags) {
    assert(e != NULL);

    struct event_base *base = event_get_base(e);
    int fd = event_get_fd(e);
    if (event_del(e) == -1) return false;
    event_set(e, fd, new_flags, idle_event_handler, NULL);
    event_base_set(base, e);
    if (event_add(e, 0) == -1) return false;
    return true;
}

static bool update_event(conn *c, const int new_flags) {
    assert(c != NULL);

    struct event_base *base = c->event.ev_base;
    if (c->ev_flags == new_flags)
        return true;
    if (event_del(&c->event) == -1) return false;
    event_set(&c->event, c->sfd, new_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = new_flags;
    if (event_add(&c->event, 0) == -1) return false;
    return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        }
        else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }

    if (do_accept) {
        STATS_LOCK();
        stats.accepting_conns = true;
        STATS_UNLOCK();
    } else {
        STATS_LOCK();
        stats.accepting_conns = false;
        stats.listen_disabled_num++;
        STATS_UNLOCK();
        allow_new_conns = false;
        maxconns_handler(-42, 0, 0);
    }
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c) {
    assert(c != NULL);

    if (c->msgcurr < c->msgused &&
            c->msglist[c->msgcurr].msg_iovlen == 0) {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused) {
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];

        res = sendmsg(c->sfd, m, 0);
        if (res > 0) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_written += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len) {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0) {
                m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Couldn't update event\n");
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0)
            perror("Failed to write, and not due to blocking");

        if (IS_UDP(c->transport))
            conn_set_state(c, conn_read);
        else
            conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    } else {
        return TRANSMIT_COMPLETE;
    }
}

static void drive_machine(conn *c) {
    bool stop = false;
    int sfd;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res;
    const char *str;
#ifdef HAVE_ACCEPT4
    static int  use_accept4 = 1;
#else
    static int  use_accept4 = 0;
#endif

    int lid;

    debug("driving machine... c->state=%d\n", c->state);
    assert(c != NULL);

    while (!stop) {

        switch(c->state) {
        case conn_listening:
            debug("case conn_listening @ %p\n", (void *)c);
            addrlen = sizeof(addr);
#ifdef HAVE_ACCEPT4
            if (use_accept4) {
                sfd = accept4(c->sfd, (struct sockaddr *)&addr, &addrlen, SOCK_NONBLOCK);
            } else {
                sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen);
            }
#else
            sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen);
#endif
            debug("listen got fd %d\n", sfd);
            if (sfd == -1) {
                if (use_accept4 && errno == ENOSYS) {
                    use_accept4 = 0;
                    continue;
                }
                perror(use_accept4 ? "accept4()" : "accept()");
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    /* these are transient, so don't log anything */
                    stop = true;
                } else if (errno == EMFILE) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Too many open connections\n");
                    accept_new_conns(false);
                    stop = true;
                } else {
                    perror("accept()");
                    stop = true;
                }
                break;
            }
            if (!use_accept4) {
                if (fcntl(sfd, F_SETFL, fcntl(sfd, F_GETFL) | O_NONBLOCK) < 0) {
                    perror("setting O_NONBLOCK");
                    close(sfd);
                    break;
                }
            }

            if (settings.maxconns_fast &&
                stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1) {
                str = "ERROR Too many open connections\r\n";
                res = write(sfd, str, strlen(str));
                close(sfd);
                STATS_LOCK();
                stats.rejected_conns++;
                STATS_UNLOCK();
            } else {
                dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST,
                                     DATA_BUFFER_SIZE, tcp_transport);
            }

            stop = true;
            break;

        case conn_waiting:
            debug("case conn_waiting @ %p\n", (void *)c);
            if (!update_event(c, EV_READ | EV_PERSIST)) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Couldn't update event\n");
                debug("case conn_waiting: could not update event\n");
                conn_set_state(c, conn_closing);
                break;
            }

            conn_set_state(c, conn_read);
            stop = true;
            break;

        case conn_read:
            debug("case conn_read @ %p\n", (void *)c);
            debug("conn_read: which=%x, (%x, %x, %x)\n", c->which,
                    EV_READ, EV_WRITE, EV_PERSIST);
            res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);
            debug("case conn_read: done\n");

            switch (res) {
            case READ_NO_DATA_RECEIVED:
                debug("case con_read: waiting\n");
                conn_set_state(c, conn_waiting);
                break;
            case READ_DATA_RECEIVED:
                conn_set_state(c, conn_parse_cmd);
                break;
            case READ_ERROR:
                debug("case con_read: error\n");
                conn_set_state(c, conn_closing); /* failure occurs here */
                break;
            case READ_MEMORY_ERROR: /* Failed to allocate more memory */
                /* State already set by try_read_network */
                break;
            }
            break;

        case conn_parse_cmd :
            debug("case conn_parse_cmd @ %p\n", (void *)c);
            if (try_read_command(c) == 0) {
                /* wee need more data! */
                conn_set_state(c, conn_waiting);
            }

            break;

        case conn_new_cmd:
            /* Only process nreqs at a time to avoid starving other
               connections */

            debug("case conn_new_cmd @ %p\n", (void *)c);
            --nreqs;
            if (nreqs >= 0) {
                reset_cmd_handler(c);
            } else {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.conn_yields++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                if (c->rbytes > 0) {
                    /* We have already read in data into the input buffer,
                       so libevent will most likely not signal read events
                       on the socket (unless more data is available. As a
                       hack we should just put in a request to write data,
                       because that should be possible ;-)
                    */
                    if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                        if (settings.verbose > 0)
                            fprintf(stderr, "Couldn't update event\n");
                        conn_set_state(c, conn_closing);
                        break;
                    }
                }
                stop = true;
            }
            break;

        case conn_recovery_scatter_nread:
        case conn_recovery_nread:
        case conn_recovery_gather_nread:
        case conn_nread:
            debug("case conn_nread @ %p\n", (void *)c);
            if (c->rlbytes == 0) {
                if (c->state == conn_nread) {
                    debug("case conn_nread\n");
                    complete_nread(c);
                } else if (c->state == conn_recovery_nread) {
                    debug("complete_recovery_nread\n");
                    complete_recovery_nread(c);
                } else if (c->state == conn_recovery_gather_nread) {
                    debug("complete_recovery_gather_nread\n");
                    complete_recovery_gather_nread(c);
                } else if (c->state == conn_recovery_scatter_nread) {
                    debug("complete_recovery_scatter_nread\n");
                    complete_recovery_scatter_nread(c);
                } else {
                    assert(0);
                }
                break;
            }

            /* Check if rbytes < 0, to prevent crash */
            if (c->rlbytes < 0) {
                if (settings.verbose) {
                    fprintf(stderr, "Invalid rlbytes to read: len %d\n", c->rlbytes);
                }
                conn_set_state(c, conn_closing);
                break;
            }

            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
                if (c->ritem != c->rcurr) {
                    memmove(c->ritem, c->rcurr, tocopy);
                }
                c->ritem += tocopy;
                c->rlbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                if (c->rlbytes == 0) {
                    break;
                }
            }

            /*  now try reading from the socket */
            res = read(c->sfd, c->ritem, c->rlbytes);
            if (res > 0) {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                if (c->rcurr == c->ritem) {
                    c->rcurr += res;
                }
                c->ritem += res;
                c->rlbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
#ifdef ENABLE_COCYTUS
                if (!is_peer_conn(c)) {
                    conn_set_state(c, conn_closing);
                } else {
                    conn_set_state(c, conn_waiting);
                }
#else
                conn_set_state(c, conn_closing);
#endif
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0) {
                fprintf(stderr, "Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                        errno, strerror(errno),
                        (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                        (int)c->rlbytes, (int)c->rsize);
            }
            conn_set_state(c, conn_closing);
            break;

        case conn_swallow:
            /* we are reading sbytes and throwing them away */
            if (c->sbytes == 0) {
                conn_set_state(c, conn_new_cmd);
                break;
            }

            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
                c->sbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                break;
            }

            /*  now try reading from the socket */
            res = read(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
            if (res > 0) {
#ifdef ENABLE_COCYTUS
                if (c->thread)
#endif
                {
                    pthread_mutex_lock(&c->thread->stats.mutex);
                    c->thread->stats.bytes_read += res;
                    pthread_mutex_unlock(&c->thread->stats.mutex);
                }
                c->sbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
#ifdef ENABLE_COCYTUS
                if (!is_peer_conn(c)) {
                    conn_set_state(c, conn_closing);
                } else {
                    conn_set_state(c, conn_waiting);
                }
#else
                conn_set_state(c, conn_closing);
#endif
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0)
                fprintf(stderr, "Failed to read, and not due to blocking\n");
            conn_set_state(c, conn_closing);
            break;

        case conn_write:
            /*
             * We want to write out a simple response. If we haven't already,
             * assemble it into a msgbuf list (this will be a single-entry
             * list for TCP or a two-entry list for UDP).
             */
            debug("case conn_write @ %p\n", (void *)c);
            if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1)) {
                if (add_iov(c, c->wcurr, c->wbytes) != 0) {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Couldn't build response\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
            }

            /* fall through... */

        case conn_mwrite:
            debug("case conn_mwrite @ %p\n", (void *)c);
          if (IS_UDP(c->transport) && c->msgcurr == 0 && build_udp_headers(c) != 0) {
            if (settings.verbose > 0)
              fprintf(stderr, "Failed to build UDP headers\n");
            conn_set_state(c, conn_closing);
            break;
          }
            switch (transmit(c)) {
            case TRANSMIT_COMPLETE:
                if (c->is_write) {
                    total_r_rep_bt_w += r_rep_cnt - c->start_r_rep;
                    total_r_req_bt_w += r_req_cnt - c->start_r_req;
                    w_rep_cnt++;
                } else {
                    total_w_rep_bt_r += w_rep_cnt - c->start_w_rep;
                    total_w_req_bt_r += w_req_cnt - c->start_w_req;
                    r_rep_cnt++;
                }
                if (c->state == conn_mwrite) {
                    conn_release_items(c);
                    /* XXX:  I don't know why this wasn't the general case */
                    if(c->protocol == binary_prot) {
                        conn_set_state(c, c->write_and_go);
                    } else {
                        conn_set_state(c, conn_new_cmd);
                    }
                } else if (c->state == conn_write) {
                    if (c->write_and_free) {
                        free(c->write_and_free);
                        c->write_and_free = 0;
                    }
                    conn_set_state(c, c->write_and_go);
                } else {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Unexpected state %d\n", c->state);
                    conn_set_state(c, conn_closing);
                }
                break;

            case TRANSMIT_INCOMPLETE:
            case TRANSMIT_HARD_ERROR:
                break;                   /* Continue in state machine. */

            case TRANSMIT_SOFT_ERROR:
                stop = true;
                break;
            }
            break;

        case conn_closing: /* may be caused by failure */
#ifdef ENABLE_COCYTUS
            // if c is one of the peers
            lid = get_real_peer_lid_by_conn(c);
            if (lid == -1) {
                // a client fails
            } else {
                if (peers[lid].state == peer_lost) goto end;
                peers[lid].state = peer_lost;
                debug("==shard conn %d closing =====\n", lid);

                struct timespec ts;
                clock_gettime(CLOCK_MONOTONIC, &ts);
                printf("%d fails at %lld s %lld ns\n", lid,
                        (long long)ts.tv_sec, (long long)ts.tv_nsec);


                int sub_leader = -1;
                if (lid >= settings.nshard) {
                    // parity fail
                    queue_remove(&leader_ring, lid);
                    if (recovery_leader == lid) {
                        // update recovery_leader
                        recovery_leader = queue_find_first(&leader_ring);
                    }
                    // wake up all clients' conns that are waiting for the failed parity
                    int i;
                    for (i=rep_queue->tail; i<rep_queue->head; ++i) {
                        struct rep_queue_item *rqit =
                            &rep_queue->items[i % rep_queue->cap];
                        struct conn *conn = (struct conn *)rqit->conn_ptr;
                        if (conn->ack & (1<<lid)) {
                            conn->ack &= ~(1<<lid);
                            if (conn->ack == 0) {
                                event_active(&conn->event, EV_WRITE, 0);
                            }
                        }
                    }
                    if (peers[lid].sub_by_or_as_lid != lid) {
                        int suber = queue_dequeue(&leader_ring);
                        int subee = peers[lid].sub_by_or_as_lid;
                        peers[suber].sub_by_or_as_lid = subee;
                        peers[subee].sub_by_or_as_lid = suber;

                        while (pac_queue_size(lid)) {
                            uint64_t addr = pac_queue_pop(lid);
                            debug(BG_R"maddr to ec_free %"PRIu64" in case conn_closing *prealloc*\n"BG_D, addr);
                            ec_free(&ecallocs[subee], addr);
                        }

                        if (suber == settings.lid) {
                            assert(IS_PARITY_SERVER);
                            sub_for(subee);
                        }
                    }
                } else {
                    // data process fail
                    sub_leader = queue_dequeue(&leader_ring);
                    peers[lid].sub_by_or_as_lid = sub_leader;
                    peers[sub_leader].sub_by_or_as_lid = lid;

                    debug("========= %d now stands for %d ======\n", sub_leader, lid);
                    if (sub_leader == settings.lid) {
                        assert(IS_PARITY_SERVER);
                        sub_for(lid);
                    }
                }
            }
end:
#endif
            if (IS_UDP(c->transport))
                conn_cleanup(c);
            else
                conn_close(c);
            stop = true;
            break;

        case conn_closed:
            /* This only happens if dormando is an idiot. */
            abort();
            break;

        case conn_max_state:
            assert(false);
            break;

#ifdef ENABLE_COCYTUS
        case conn_write_to_peer:
            debug("case conn_write_to_peer @ %p\n", (void *)c);
            lid = get_peer_lid_by_conn(c);
            debug("write to lid =%d\n", lid);
            //pthread_mutex_lock(&peers[lid].mutex);
            if (peers[lid].state == peer_connected) {
                int r = conn_flush_wbuf(c);
                if (r != 0) {
                    if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                        assert(0);
                    }
                }
            }
            //pthread_mutex_unlock(&peers[lid].mutex);
            stop = true;
            break;
        case conn_peer_connect:
            debug("case conn_peer_connect @ %p\n", (void *)c);
            /// XXX:
            //send_short_msg(c, "peerconn", "");
            //out_stringf(c, "peerconn %d %d", settings.gid, settings.lid);
            send_msgf(c, "peerconn %d %d", settings.gid, settings.lid);

            // set state
            lid = get_peer_lid_by_conn(c);
            debug("lid=>%d\n", lid);
            if (lid == -1) {
                fprintf(stderr, "error get peer lid\n");
                exit(-1);
            }
            debug("connect: lid =%d\n", lid);
            assert(peers[lid].state == peer_connecting);
            peers[lid].state = peer_connected;
            //pthread_mutex_unlock(&peers[lid].mutex);
            conn_set_state(c, conn_write_to_peer);

            //stop = true;
            break;
        case conn_pipe_send:
        case conn_pipe_recv:
            break;
        case conn_pre_waiting_ack:
            if (!update_event(c, EV_PERSIST)) {
                conn_set_state(c, conn_closing);
            }
            conn_set_state(c, c->next_state);
            debug("======= conn_pre_waiting_ack ======\n");
            stop = true;
            break;
        case conn_recovery_complete_for_get:
            debug("===========> all recovry complete <=========\n");
            /*
             * Construct the response. Each hit adds three elements to the
             * outgoing data list:
             *   "VALUE "
             *   key
             *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
             */
            item *it = c->item;
            assert(it->refcount > 0);
            assert_data_availability(it->addr, it->nbytes);
            switch_hashtable(sub_as_lid);
            item *itt = item_get(ITEM_key(it), it->nkey);
            if (itt != it) {
                item_remove(it);
                it = itt;
                c->item = itt;
            } else {
                item_remove(itt);
            }

            if (c->vnbytes < it->nbytes || (c->vnbytes >> 1) > it->nbytes) {
                if (c->vbuf) free(c->vbuf);
                c->vbuf = malloc(it->nbytes);
                c->vnbytes = it->nbytes;
            }
            memcpy(c->vbuf, ecmem_get(sub_ecmem, it->addr), it->nbytes);
            if (add_iov(c, "VALUE ", 6) != 0 ||
                    add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                    add_iov(c, ITEM_suffix(it), it->nsuffix) != 0 ||
                    // FIXME: should add to iov in multiple time
                    //add_iovs_from_recovery(c, it->addr, it->nbytes) != 0
                    add_iov(c, c->vbuf, it->nbytes) != 0
               ) {
                item_remove(it);
                assert(0);
            } else {
                item_update(it);
                if (add_iov(c, "END\r\n", 5) != 0) {
                    out_of_memory(c, "SERVER_ERROR out of memory writing get response");
                }
                else {
                    conn_set_state(c, conn_mwrite);
                    c->msgcurr = 0;
                }
            }
            break;
        case conn_recovery_complete_for_set:

            it = c->item;
            assert(it->refcount > 0);

            assert_data_availability(it->addr, it->nbytes);
            char *addrp = ecmem_get(sub_ecmem, it->addr);

            // alloc for diff
            char *diff_base  = safe_malloc(it->nbytes + 15 + 128);
            // align to 16
            char *diff = ROUND(diff_base, 16ul);

            // move new data to diff
            memcpy(diff, c->vbuf, it->nbytes);

            // calc the real diff
            galois_w08_region_multiply(addrp, 1, it->nbytes, diff, 1);

            // send to others
            c->xid = alloc_xid++;
            struct rep_queue_item *rqit = rep_queue_add(rep_queue);
            rqit->xid = c->xid;
            rqit->conn_ptr = (uint64_t)c;
            c->ack = 0;

            int i;
            FOREACH_PARITY_LID(i) {
                if (i == settings.lid || peers[i].state == peer_lost) {
                    continue;
                }
                debug("send to others %d\n", i);
                c->ack |= 1 << i;
                parity_send(peers[i].conn_w, c, c->xid, it, diff);
                event_active(&peers[i].conn_w->event, EV_WRITE, 0);
            }

            //free(c->vbuf);
            //c->vnbytes = it->nbytes + 15 + 128;
            //c->vbuf = diff_base;
            //free(diff_base);
            c->ritem = diff_base; // a temp use

            if (c->ack == 0) {
                conn_set_state(c, conn_waiting_ack);
            } else {
                c->next_state = conn_waiting_ack;
                conn_set_state(c, conn_pre_waiting_ack);
            }
            break;
        case conn_waiting_ack:
            // all parity received the
            debug("%x\n", c->which);
            debug("===========> all parity received <=========\n");
            assert(c->ack == 0);
            // finish the complete_nread
            it = c->item;
            debug_item(it);

            rqit = rep_queue_first(rep_queue);
            //assert(rqit->xid == stable_xid + 1);
            debug("== stable_xid=%"PRIu64"   rqit->xid=%"PRIu64"   c->xid=%"PRIu64" \n",
                    stable_xid, rqit->xid, c->xid);
            assert(rqit->xid > stable_xid);
            rqit->done = 1;
            rep_queue_flush(rep_queue);

            //if (settings.lid == sub_as_lid) {
            if (!IS_PARITY_SERVER) {
                char *addrp = ecmem_get(&ecmem, it->addr);
                debug("set: update addr\n");
                // move new data in position
                memcpy(addrp, c->vbuf, it->nbytes);

                switch_hashtable(0);

                complete_nread_ascii(c);

                stable_xid = c->xid;
            } else {
                char *addrp = ecmem_get(sub_ecmem, it->addr);

                // move new data in position
                memcpy(addrp, c->vbuf, it->nbytes);

                switch_hashtable(sub_as_lid);

                complete_nread_ascii(c);

                stable_xid = c->xid;

                free(c->ritem);
                c->ritem = c->vbuf;
            }
            out_string(c, "STORED");
            break;
#endif
        }
    }

#ifdef ENABLE_COCYTUS
    if (pthread_equal(pthread_self(), worker_thread)) {
        int x;
        for (x=0; x<settings.nparity+settings.nshard; ++x) {
            if (peers[x].state == peer_connected) {
                //pthread_mutex_lock(&peers[x].mutex);
                debug("flushing: %d\n", x);
                conn_flush_wbuf(peers[x].conn_w);
                //pthread_mutex_unlock(&peers[x].mutex);
            }
        }
    }
#endif
    debug("conn %p out of driving\n", (void *)c);

    return;
}

void idle_event_handler(const int fd, const short which, void *arg)
{
    if (!sub_as_ready) return;
    int *s = &recovery_start_unit;

    int countdown = 4096;
    while (*s < end_unit && sub_flags[*s] != 0 && countdown) {
        *s += 1;
        countdown--;
    }
    if (!countdown) {
        printf("counting down too much\n");
        return;
    }
    if (*s == end_unit) {
        update_idle_event(idle_recovery_event, EV_PERSIST);
        return;
    }
    if (*s - recovery_finished_unit > TOO_MANY_RECOVERY)
	return;
    do_recovery(NULL, *s, *s);
    return;
}

void event_handler(const int fd, const short which, void *arg) {
    conn *c;

    c = (conn *)arg;
    assert(c != NULL);

    c->which = which;

    /* sanity */
    if (fd != c->sfd) {
        if (settings.verbose > 0)
            fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        conn_close(c);
        return;
    }

    drive_machine(c);

    /* wait for next event */
    return;
}

static int new_socket(struct addrinfo *ai) {
    int sfd;
    int flags;

    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 */
static int server_socket(const char *interface,
                         int port,
                         enum network_transport transport,
                         FILE *portnumber_file) {
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,
                              .ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;

    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;

    if (port == -1) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    error= getaddrinfo(interface, port_buf, &hints, &ai);
    if (error != 0) {
        if (error != EAI_SYSTEM)
          fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        else
          perror("getaddrinfo()");
        return 1;
    }

    for (next= ai; next; next= next->ai_next) {
        conn *listen_conn_add;
        if ((sfd = new_socket(next)) == -1) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            if (errno == EMFILE) {
                /* ...unless we're out of fds */
                perror("server_socket");
                exit(EX_OSERR);
            }
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        if (IS_UDP(transport)) {
            maximize_sndbuf(sfd);
        } else {
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0)
                perror("setsockopt");

            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0)
                perror("setsockopt");
        }

        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1) {
            if (errno != EADDRINUSE) {
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            close(sfd);
            continue;
        } else {
            success++;
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1) {
                perror("listen()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&
                (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6)) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (next->ai_addr->sa_family == AF_INET) {
                        fprintf(portnumber_file, "%s INET: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in.sin_port));
                    } else {
                        fprintf(portnumber_file, "%s INET6: %u\n",
                                IS_UDP(transport) ? "UDP" : "TCP",
                                ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }

        if (IS_UDP(transport)) {
            int c;

            for (c = 0; c < settings.num_threads_per_udp; c++) {
                /* Allocate one UDP file descriptor per worker thread;
                 * this allows "stats conns" to separately list multiple
                 * parallel UDP requests in progress.
                 *
                 * The dispatch code round-robins new connection requests
                 * among threads, so this is guaranteed to assign one
                 * FD to each thread.
                 */
                int per_thread_fd = c ? dup(sfd) : sfd;
                dispatch_conn_new(per_thread_fd, conn_read,
                                  EV_READ | EV_PERSIST,
                                  UDP_READ_BUFFER_SIZE, transport);
            }
        } else {
            if (!(listen_conn_add = conn_new(sfd, conn_listening,
                                             EV_READ | EV_PERSIST, 1,
                                             transport, main_base))) {
                fprintf(stderr, "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
        }
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int server_sockets(int port, enum network_transport transport,
                          FILE *portnumber_file) {
    if (settings.inter == NULL) {
        return server_socket(settings.inter, port, transport, portnumber_file);
    } else {
        // tokenize them and bind to each one of them..
        char *b;
        int ret = 0;
        char *list = strdup(settings.inter);

        if (list == NULL) {
            fprintf(stderr, "Failed to allocate memory for parsing server interface string\n");
            return 1;
        }
        for (char *p = strtok_r(list, ";,", &b);
             p != NULL;
             p = strtok_r(NULL, ";,", &b)) {
            int the_port = port;
            char *s = strchr(p, ':');
            if (s != NULL) {
                *s = '\0';
                ++s;
                if (!safe_strtol(s, &the_port)) {
                    fprintf(stderr, "Invalid port number: \"%s\"", s);
                    return 1;
                }
            }
            if (strcmp(p, "*") == 0) {
                p = NULL;
            }
            ret |= server_socket(p, the_port, transport, portnumber_file);
        }
        free(list);
        return ret;
    }
}

static int new_socket_unix(void) {
    int sfd;
    int flags;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}

static int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags =1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    /*
     * Clean up a previous socket file if we left it around
     */
    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode))
            unlink(path);
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    /*
     * the memset call clears nonstandard fields in some impementations
     * that otherwise mess things up.
     */
    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask( ~(access_mask&0777));
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind()");
        close(sfd);
        umask(old_umask);
        return 1;
    }
    umask(old_umask);
    if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        close(sfd);
        return 1;
    }
    if (!(listen_conn = conn_new(sfd, conn_listening,
                                 EV_READ | EV_PERSIST, 1,
                                 local_transport, main_base))) {
        fprintf(stderr, "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;

/* libevent uses a monotonic clock when available for event scheduling. Aside
 * from jitter, simply ticking our internal timer here is accurate enough.
 * Note that users who are setting explicit dates for expiration times *must*
 * ensure their clocks are correct before starting memcached. */
static void clock_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    static bool monotonic = false;
    static time_t monotonic_start;
#endif

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
        /* process_started is initialized to time() - 2. We initialize to 1 so
         * flush_all won't underflow during tests. */
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
            monotonic = true;
            monotonic_start = ts.tv_sec - ITEM_UPDATE_INTERVAL - 2;
        }
#endif
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    if (monotonic) {
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
            return;
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start);
        return;
    }
#endif
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        current_time = (rel_time_t) (tv.tv_sec - process_started);
    }
}

static void usage(void) {
    printf(PACKAGE " " VERSION "\n");
    printf("-p <num>      TCP port number to listen on (default: 11211)\n"
           "-U <num>      UDP port number to listen on (default: 11211, 0 is off)\n"
           "-s <file>     UNIX socket path to listen on (disables network support)\n"
           "-A            enable ascii \"shutdown\" command\n"
           "-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
           "-l <addr>     interface to listen on (default: INADDR_ANY, all addresses)\n"
           "              <addr> may be specified as host:port. If you don't specify\n"
           "              a port number, the value you specified with -p or -U is\n"
           "              used. You may specify multiple addresses separated by comma\n"
           "              or by using -l multiple times\n"

           "-d            run as a daemon\n"
           "-r            maximize core file limit\n"
           "-u <username> assume identity of <username> (only when run as root)\n"
           "-m <num>      max memory to use for items in megabytes (default: 64 MB)\n"
           "-M            return error on memory exhausted (rather than removing items)\n"
           "-c <num>      max simultaneous connections (default: 1024)\n"
           "-k            lock down all paged memory.  Note that there is a\n"
           "              limit on how much memory you may lock.  Trying to\n"
           "              allocate more than that would fail, so be sure you\n"
           "              set the limit correctly for the user you started\n"
           "              the daemon with (not for -u <username> user;\n"
           "              under sh this is done with 'ulimit -S -l NUM_KB').\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           "-vv           very verbose (also print client commands/reponses)\n"
           "-vvv          extremely verbose (also print internal state transitions)\n"
           "-h            print this help and exit\n"
           "-i            print memcached and libevent license\n"
           "-P <file>     save PID in <file>, only used with -d option\n"
           "-f <factor>   chunk size growth factor (default: 1.25)\n"
           "-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
    printf("-L            Try to use large memory pages (if available). Increasing\n"
           "              the memory page size could reduce the number of TLB misses\n"
           "              and improve the performance. In order to get large pages\n"
           "              from the OS, memcached will allocate the total item-cache\n"
           "              in one large chunk.\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \":\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n");
    printf("-t <num>      number of threads to use (default: 4)\n");
    printf("-R            Maximum number of requests per event, limits the number of\n"
           "              requests process for a given connection to prevent \n"
           "              starvation (default: 20)\n");
    printf("-C            Disable use of CAS\n");
    printf("-b            Set the backlog queue limit (default: 1024)\n");
    printf("-B            Binding protocol - one of ascii, binary, or auto (default)\n");
    printf("-I            Override the size of each slab page. Adjusts max item size\n"
           "              (default: 1mb, min: 1k, max: 128m)\n");
#ifdef ENABLE_SASL
    printf("-S            Turn on Sasl authentication\n");
#endif
    printf("-F            Disable flush_all command\n");
    printf("-o            Comma separated list of extended or experimental options\n"
           "              - (EXPERIMENTAL) maxconns_fast: immediately close new\n"
           "                connections if over maxconns limit\n"
           "              - hashpower: An integer multiplier for how large the hash\n"
           "                table should be. Can be grown at runtime if not big enough.\n"
           "                Set this based on \"STAT hash_power_level\" before a \n"
           "                restart.\n"
           "              - tail_repair_time: Time in seconds that indicates how long to wait before\n"
           "                forcefully taking over the LRU tail item whose refcount has leaked.\n"
           "                The default is 3 hours.\n"
           "              - hash_algorithm: The hash table algorithm\n"
           "                default is jenkins hash. options: jenkins, murmur3\n"
           "              - lru_crawler: Enable LRU Crawler background thread\n"
           "              - lru_crawler_sleep: Microseconds to sleep between items\n"
           "                default is 100.\n"
           "              - lru_crawler_tocrawl: Max items to crawl per slab per run\n"
           "                default is 0 (unlimited)\n"
           );
#ifdef ENABLE_COCYTUS
    printf("-g <string>   The path to the configuration file\n");
    printf("-X <num>      The group id of this node (in config file)\n");
    printf("-x <num>      The local(data/parity) id of this node (in config file)\n");
#endif
    return;
}

static void usage_license(void) {
    printf(PACKAGE " " VERSION "\n\n");
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    "\n"
    "\n"
    "This product includes software developed by Niels Provos.\n"
    "\n"
    "[ libevent ]\n"
    "\n"
    "Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "1. Redistributions of source code must retain the above copyright\n"
    "   notice, this list of conditions and the following disclaimer.\n"
    "2. Redistributions in binary form must reproduce the above copyright\n"
    "   notice, this list of conditions and the following disclaimer in the\n"
    "   documentation and/or other materials provided with the distribution.\n"
    "3. All advertising materials mentioning features or use of this software\n"
    "   must display the following acknowledgement:\n"
    "      This product includes software developed by Niels Provos.\n"
    "4. The name of the author may not be used to endorse or promote products\n"
    "   derived from this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
    "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
    "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
    "IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
    "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
    "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
    "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static void save_pid(const char *pid_file) {
    FILE *fp;
    if (access(pid_file, F_OK) == 0) {
        if ((fp = fopen(pid_file, "r")) != NULL) {
            char buffer[1024];
            if (fgets(buffer, sizeof(buffer), fp) != NULL) {
                unsigned int pid;
                if (safe_strtoul(buffer, &pid) && kill((pid_t)pid, 0) == 0) {
                    fprintf(stderr, "WARNING: The pid file contained the following (running) pid: %u\n", pid);
                }
            }
            fclose(fp);
        }
    }

    /* Create the pid file first with a temporary name, then
     * atomically move the file to the real name to avoid a race with
     * another process opening the file to read the pid, but finding
     * it empty.
     */
    char tmp_pid_file[1024];
    snprintf(tmp_pid_file, sizeof(tmp_pid_file), "%s.tmp", pid_file);

    if ((fp = fopen(tmp_pid_file, "w")) == NULL) {
        vperror("Could not open the pid file %s for writing", tmp_pid_file);
        return;
    }

    fprintf(fp,"%ld\n", (long)getpid());
    if (fclose(fp) == -1) {
        vperror("Could not close the pid file %s", tmp_pid_file);
    }

    if (rename(tmp_pid_file, pid_file) != 0) {
        vperror("Could not rename the pid file from %s to %s",
                tmp_pid_file, pid_file);
    }
}

static void remove_pidfile(const char *pid_file) {
  if (pid_file == NULL)
      return;

  if (unlink(pid_file) != 0) {
      vperror("Could not remove the pid file %s", pid_file);
  }

}

static void sigusr1_handler(const int sig);
static void sigusr1_handler(const int sig)
{
    char fn[20];
    sprintf(fn, "sigusr1_%d_%d", settings.gid, settings.lid);
    FILE *f = fopen(fn, "w");
    fprintf(f, "total_w_req_bt_r: %"PRIu64"\n", total_w_req_bt_r);
    fprintf(f, "total_w_rep_bt_r: %"PRIu64"\n", total_w_rep_bt_r);
    fprintf(f, "total_r_req_bt_w: %"PRIu64"\n", total_r_req_bt_w);
    fprintf(f, "total_r_rep_bt_w: %"PRIu64"\n", total_r_rep_bt_w);
    fprintf(f, "w_req_cnt: %"PRIu64"\n", w_req_cnt);
    fprintf(f, "w_rep_cnt: %"PRIu64"\n", w_rep_cnt);
    fprintf(f, "r_req_cnt: %"PRIu64"\n", r_req_cnt);
    fprintf(f, "r_rep_cnt: %"PRIu64"\n", r_rep_cnt);
    fclose(f);
    f = NULL;
}

static void sig_handler(const int sig) {
    printf("SIGINT handled.\n");
    exit(EXIT_SUCCESS);
}

#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif


/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            fprintf(stderr, "Failed to set large pages: %s\n",
                    strerror(errno));
            fprintf(stderr, "Will use default page size\n");
        } else {
            ret = 0;
        }
    } else {
        fprintf(stderr, "Failed to get supported pagesizes: %s\n",
                strerror(errno));
        fprintf(stderr, "Will use default page size\n");
    }

    return ret;
#else
    return -1;
#endif
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
    /* One of our biggest problems is old and bogus libevents */
    const char *ever = event_get_version();
    if (ever != NULL) {
        if (strncmp(ever, "1.", 2) == 0) {
            /* Require at least 1.3 (that's still a couple of years old) */
            if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
                fprintf(stderr, "You are using libevent %s.\nPlease upgrade to"
                        " a more recent version (1.3 or newer)\n",
                        event_get_version());
                return false;
            }
        }
    }

    return true;
}

int main (int argc, char **argv) {
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    bool preallocate = false;
    int maxcore = 0;
    char *username = NULL;
    char *pid_file = NULL;
    struct passwd *pw;
    struct rlimit rlim;
    char *buf;
    char unit = '\0';
    int size_max = 0;
    int retval = EXIT_SUCCESS;
    /* listening sockets */
    static int *l_socket = NULL;

    /* udp socket */
    static int *u_socket = NULL;
    bool protocol_specified = false;
    bool tcp_specified = false;
    bool udp_specified = false;
    enum hashfunc_type hash_type = JENKINS_HASH;
    uint32_t tocrawl;

    char *subopts;
    char *subopts_value;
    enum {
        MAXCONNS_FAST = 0,
        HASHPOWER_INIT,
        SLAB_REASSIGN,
        SLAB_AUTOMOVE,
        TAIL_REPAIR_TIME,
        HASH_ALGORITHM,
        LRU_CRAWLER,
        LRU_CRAWLER_SLEEP,
        LRU_CRAWLER_TOCRAWL
    };
    char *const subopts_tokens[] = {
        [MAXCONNS_FAST] = "maxconns_fast",
        [HASHPOWER_INIT] = "hashpower",
        [SLAB_REASSIGN] = "slab_reassign",
        [SLAB_AUTOMOVE] = "slab_automove",
        [TAIL_REPAIR_TIME] = "tail_repair_time",
        [HASH_ALGORITHM] = "hash_algorithm",
        [LRU_CRAWLER] = "lru_crawler",
        [LRU_CRAWLER_SLEEP] = "lru_crawler_sleep",
        [LRU_CRAWLER_TOCRAWL] = "lru_crawler_tocrawl",
        NULL
    };

    if (!sanitycheck()) {
        return EX_OSERR;
    }

    /* handle SIGINT */
    signal(SIGINT, sig_handler);
    signal(SIGUSR1, sigusr1_handler);

    /* init settings */
    settings_init();

    /* set stderr non-buffering (for running under, say, daemontools) */
    setbuf(stderr, NULL);

    /* process arguments */
    while (-1 != (c = getopt(argc, argv,
          "a:"  /* access mask for unix socket */
          "A"  /* enable admin shutdown commannd */
          "p:"  /* TCP port number to listen on */
          "s:"  /* unix socket path to listen on */
          "U:"  /* UDP port number to listen on */
          "m:"  /* max memory to use for items in megabytes */
          "M"   /* return error on memory exhausted */
          "c:"  /* max simultaneous connections */
          "k"   /* lock down all paged memory */
          "hi"  /* help, licence info */
          "r"   /* maximize core file limit */
          "v"   /* verbose */
          "d"   /* daemon mode */
          "l:"  /* interface to listen on */
          "u:"  /* user identity to run as */
          "P:"  /* save PID in file */
          "f:"  /* factor? */
          "n:"  /* minimum space allocated for key+value+flags */
          "t:"  /* threads */
          "D:"  /* prefix delimiter? */
          "L"   /* Large memory pages */
          "R:"  /* max requests per event */
          "C"   /* Disable use of CAS */
          "b:"  /* backlog queue limit */
          "B:"  /* Binding protocol */
          "I:"  /* Max item size */
          "S"   /* Sasl ON */
          "F"   /* Disable flush_all */
          "o:"  /* Extended generic options */
#ifdef ENABLE_COCYTUS
          "X:"  /* group id */
          "x:"  /* local id */
          "g:"  /* config file */
#endif
        ))) {
        switch (c) {
        case 'A':
            /* enables "shutdown" command */
            settings.shutdown_command = true;
            break;

        case 'a':
            /* access for unix domain socket, as octal mask (like chmod)*/
            settings.access= strtol(optarg,NULL,8);
            break;

        case 'U':
            settings.udpport = atoi(optarg);
            udp_specified = true;
            break;
        case 'p':
            settings.port = atoi(optarg);
            tcp_specified = true;
            break;
        case 's':
            settings.socketpath = optarg;
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
            break;
        case 'M':
            settings.evict_to_free = 0;
            break;
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            settings.verbose++;
            break;
        case 'l':
            if (settings.inter != NULL) {
                size_t len = strlen(settings.inter) + strlen(optarg) + 2;
                char *p = malloc(len);
                if (p == NULL) {
                    fprintf(stderr, "Failed to allocate memory\n");
                    return 1;
                }
                snprintf(p, len, "%s,%s", settings.inter, optarg);
                free(settings.inter);
                settings.inter = p;
            } else {
                settings.inter= strdup(optarg);
            }
            break;
        case 'd':
            do_daemonize = true;
            break;
        case 'r':
            maxcore = 1;
            break;
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event == 0) {
                fprintf(stderr, "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
        case 'P':
            pid_file = optarg;
            break;
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                fprintf(stderr, "Factor must be greater than 1\n");
                return 1;
            }
            break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                fprintf(stderr, "Chunk size must be greater than 0\n");
                return 1;
            }
            break;
        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                fprintf(stderr, "Number of threads must be greater than 0\n");
                return 1;
            }
            /* There're other problems when you get above 64 threads.
             * In the future we should portably detect # of cores for the
             * default.
             */
            if (settings.num_threads > 64) {
                fprintf(stderr, "WARNING: Setting a high number of worker"
                                "threads is not recommended.\n"
                                " Set this value to the number of cores in"
                                " your machine or less.\n");
            }
            break;
        case 'D':
            if (! optarg || ! optarg[0]) {
                fprintf(stderr, "No delimiter specified\n");
                return 1;
            }
            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;
        case 'L' :
            if (enable_large_pages() == 0) {
                preallocate = true;
            } else {
                fprintf(stderr, "Cannot enable large pages on this system\n"
                    "(There is no Linux support as of this version)\n");
                return 1;
            }
            break;
        case 'C' :
            settings.use_cas = false;
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                fprintf(stderr, "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'I':
            buf = strdup(optarg);
            unit = buf[strlen(buf)-1];
            if (unit == 'k' || unit == 'm' ||
                unit == 'K' || unit == 'M') {
                buf[strlen(buf)-1] = '\0';
                size_max = atoi(buf);
                if (unit == 'k' || unit == 'K')
                    size_max *= 1024;
                if (unit == 'm' || unit == 'M')
                    size_max *= 1024 * 1024;
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(buf);
            }
            if (settings.item_size_max < 1024) {
                fprintf(stderr, "Item max size cannot be less than 1024 bytes.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024 * 128) {
                fprintf(stderr, "Cannot set item size limit higher than 128 mb.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024) {
                fprintf(stderr, "WARNING: Setting item max size above 1MB is not"
                    " recommended!\n"
                    " Raising this limit increases the minimum memory requirements\n"
                    " and will decrease your memory efficiency.\n"
                );
            }
            free(buf);
            break;
        case 'S': /* set Sasl authentication to true. Default is false */
#ifndef ENABLE_SASL
            fprintf(stderr, "This server is not built with SASL support.\n");
            exit(EX_USAGE);
#endif
            settings.sasl = true;
            break;
        case 'F' :
            settings.flush_enabled = false;
            break;
#ifdef ENABLE_COCYTUS
        case 'g':
            //parse_config_file(optarg);
            settings.config_file = strdup(optarg);
            break;
        case 'X':
            settings.gid = atoi(optarg); // group id
            break;
        case 'x':
            settings.lid = atoi(optarg); // local id
            break;
#endif
        case 'o': /* It's sub-opts time! */
            subopts = optarg;

            while (*subopts != '\0') {

            switch (getsubopt(&subopts, subopts_tokens, &subopts_value)) {
            case MAXCONNS_FAST:
                settings.maxconns_fast = true;
                break;
            case HASHPOWER_INIT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for hashpower\n");
                    return 1;
                }
                settings.hashpower_init = atoi(subopts_value);
                if (settings.hashpower_init < 12) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too low\n",
                        settings.hashpower_init);
                    return 1;
                } else if (settings.hashpower_init > 64) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too high\n"
                        "Choose a value based on \"STAT hash_power_level\" from a running instance\n",
                        settings.hashpower_init);
                    return 1;
                }
                break;
            case SLAB_REASSIGN:
                settings.slab_reassign = true;
                break;
            case SLAB_AUTOMOVE:
                if (subopts_value == NULL) {
                    settings.slab_automove = 1;
                    break;
                }
                settings.slab_automove = atoi(subopts_value);
                if (settings.slab_automove < 0 || settings.slab_automove > 2) {
                    fprintf(stderr, "slab_automove must be between 0 and 2\n");
                    return 1;
                }
                break;
            case TAIL_REPAIR_TIME:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for tail_repair_time\n");
                    return 1;
                }
                settings.tail_repair_time = atoi(subopts_value);
                if (settings.tail_repair_time < 10) {
                    fprintf(stderr, "Cannot set tail_repair_time to less than 10 seconds\n");
                    return 1;
                }
                break;
            case HASH_ALGORITHM:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing hash_algorithm argument\n");
                    return 1;
                };
                if (strcmp(subopts_value, "jenkins") == 0) {
                    hash_type = JENKINS_HASH;
                } else if (strcmp(subopts_value, "murmur3") == 0) {
                    hash_type = MURMUR3_HASH;
                } else {
                    fprintf(stderr, "Unknown hash_algorithm option (jenkins, murmur3)\n");
                    return 1;
                }
                break;
            case LRU_CRAWLER:
                if (start_item_crawler_thread() != 0) {
                    fprintf(stderr, "Failed to enable LRU crawler thread\n");
                    return 1;
                }
                break;
            case LRU_CRAWLER_SLEEP:
                settings.lru_crawler_sleep = atoi(subopts_value);
                if (settings.lru_crawler_sleep > 1000000 || settings.lru_crawler_sleep < 0) {
                    fprintf(stderr, "LRU crawler sleep must be between 0 and 1 second\n");
                    return 1;
                }
                break;
            case LRU_CRAWLER_TOCRAWL:
                if (!safe_strtoul(subopts_value, &tocrawl)) {
                    fprintf(stderr, "lru_crawler_tocrawl takes a numeric 32bit value\n");
                    return 1;
                }
                settings.lru_crawler_tocrawl = tocrawl;
                break;
            default:
                printf("Illegal suboption \"%s\"\n", subopts_value);
                return 1;
            }

            }
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }
#ifdef ENABLE_COCYTUS
    if (settings.config_file) {
        parse_config_file(settings.config_file);
        free(settings.config_file);
        settings.config_file = NULL;
    }
    matrix = (int *)reed_sol_big_vandermonde_distribution_matrix(
            settings.nshard+settings.nparity, settings.nshard, 8);
#endif

    if (hash_init(hash_type) != 0) {
        fprintf(stderr, "Failed to initialize hash_algorithm!\n");
        exit(EX_USAGE);
    }


    /*
     * Use one workerthread to serve each UDP port if the user specified
     * multiple ports
     */
    if (settings.inter != NULL && strchr(settings.inter, ',')) {
        settings.num_threads_per_udp = 1;
    } else {
        settings.num_threads_per_udp = settings.num_threads;
    }

    if (settings.sasl) {
        if (!protocol_specified) {
            settings.binding_protocol = binary_prot;
        } else {
            if (settings.binding_protocol != binary_prot) {
                fprintf(stderr, "ERROR: You cannot allow the ASCII protocol while using SASL.\n");
                exit(EX_USAGE);
            }
        }
    }

    if (tcp_specified && !udp_specified) {
        settings.udpport = settings.port;
    } else if (udp_specified && !tcp_specified) {
        settings.port = settings.udpport;
    }

    if (maxcore != 0) {
        struct rlimit rlim_new;
        /*
         * First try raising to infinity; if that fails, try bringing
         * the soft limit to the hard.
         */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0) {
                /* failed. try raising just to the old max */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        /*
         * getrlimit again to see what we ended up with. Only fail if
         * the soft limit ends up 0, because then no core files will be
         * created at all.
         */

        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0) {
            fprintf(stderr, "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }

    /*
     * If needed, increase rlimits to allow as many connections
     * as needed.
     */

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        fprintf(stderr, "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        rlim.rlim_cur = settings.maxconns;
        rlim.rlim_max = settings.maxconns;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            fprintf(stderr, "failed to set rlimit for open files. Try starting as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }

    /* lose root privileges if we have them */
    if (getuid() == 0 || geteuid() == 0) {
        if (username == 0 || *username == '\0') {
            fprintf(stderr, "can't run as root without the -u switch\n");
            exit(EX_USAGE);
        }
        if ((pw = getpwnam(username)) == 0) {
            fprintf(stderr, "can't find the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0) {
            fprintf(stderr, "failed to assume identity of user %s\n", username);
            exit(EX_OSERR);
        }
    }

    /* Initialize Sasl if -S was specified */
    if (settings.sasl) {
        init_sasl();
    }

    /* daemonize if requested */
    /* if we want to ensure our ability to dump core, don't chdir to / */
    if (do_daemonize) {
        if (sigignore(SIGHUP) == -1) {
            perror("Failed to ignore SIGHUP");
        }
        if (daemonize(maxcore, settings.verbose) == -1) {
            fprintf(stderr, "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }

    /* lock paged memory if needed */
    if (lock_memory) {
#ifdef HAVE_MLOCKALL
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0) {
            fprintf(stderr, "warning: -k invalid, mlockall() failed: %s\n",
                    strerror(errno));
        }
#else
        fprintf(stderr, "warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }

    /* initialize main thread libevent instance */
    main_base = event_init();

    /* initialize other stuff */
    stats_init();
    assoc_init(settings.hashpower_init);
    conn_init();
    slabs_init(settings.maxbytes, settings.factor, preallocate);

    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
    /* start up worker threads if MT mode */
#ifdef ENABLE_COCYTUS
    assert(settings.num_threads == 1);
    pthread_barrier_init(&thread_barrier, NULL, 2);
#endif
    thread_init(settings.num_threads, main_base);

    if (start_assoc_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }

    if (settings.slab_reassign &&
        start_slab_maintenance_thread() == -1) {
        exit(EXIT_FAILURE);
    }

    /* Run regardless of initializing it later */
    init_lru_crawler();

    /* initialise clock event */
    clock_handler(0, 0, 0);

#ifdef ENABLE_COCYTUS
    ecmem_init(&ecmem, MEMSIZE);
    recovery_init(&recovery);
    bop_queue_init();
    pac_queues_init(NPEER);
    cocytus_init();
#endif
    /* create unix mode sockets after dropping privileges */
    if (settings.socketpath != NULL) {
        errno = 0;
        if (server_socket_unix(settings.socketpath,settings.access)) {
            vperror("failed to listen on UNIX socket: %s", settings.socketpath);
            exit(EX_OSERR);
        }
    }

    /* create the listening socket, bind it, and init */
    if (settings.socketpath == NULL) {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        char temp_portnumber_filename[PATH_MAX];
        FILE *portnumber_file = NULL;

        if (portnumber_filename != NULL) {
            snprintf(temp_portnumber_filename,
                     sizeof(temp_portnumber_filename),
                     "%s.lck", portnumber_filename);

            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL) {
                fprintf(stderr, "Failed to open \"%s\": %s\n",
                        temp_portnumber_filename, strerror(errno));
            }
        }

        errno = 0;
        if (settings.port && server_sockets(settings.port, tcp_transport,
                                           portnumber_file)) {
            vperror("failed to listen on TCP port %d", settings.port);
            exit(EX_OSERR);
        } else {
            printf("listening to port: %d\n", settings.port);
        }

        /*
         * initialization order: first create the listening sockets
         * (may need root on low ports), then drop root if needed,
         * then daemonise if needed, then init libevent (in some cases
         * descriptors created by libevent wouldn't survive forking).
         */

        /* create the UDP listening socket and bind it */
        errno = 0;
        if (settings.udpport && server_sockets(settings.udpport, udp_transport,
                                              portnumber_file)) {
            vperror("failed to listen on UDP port %d", settings.udpport);
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename, portnumber_filename);
        }
    }

    /* Give the sockets a moment to open. I know this is dumb, but the error
     * is only an advisory.
     */
    usleep(1000);
    if (stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1) {
        fprintf(stderr, "Maxconns setting is too low, use -c to increase.\n");
        exit(EXIT_FAILURE);
    }

    if (pid_file != NULL) {
        save_pid(pid_file);
    }

    /* Drop privileges no longer needed */
    drop_privileges();

#ifdef ENABLE_COCYTUS
    debug("enter main loop for listening\n");
    pthread_barrier_wait(&thread_barrier);
#endif
    /* enter the event loop */
    if (event_base_loop(main_base, 0) != 0) {
        retval = EXIT_FAILURE;
    }

    stop_assoc_maintenance_thread();

    /* remove the PID file if we're a daemon */
    if (do_daemonize)
        remove_pidfile(pid_file);
    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);
    if (l_socket)
      free(l_socket);
    if (u_socket)
      free(u_socket);

    return retval;
}

#ifdef ENABLE_COCYTUS

static int parse_line(FILE *f, char **line, size_t *size) {
    int n;
    while ((n = getline(line, size, f)) != -1) {
        if ((*line)[0] == '#') continue;
        if ((*line)[0] == '\r' || (*line)[0] == '\n') continue;
        return n;
    }
    return -1;
}

void parse_config_file(char *conf) {
    FILE *f = fopen(conf, "r");

    char *line = NULL;
    size_t linesize = 0;

    parse_line(f, &line, &linesize);
    settings.nnode = atoi(line);
    parse_line(f, &line, &linesize);
    settings.nshard = atoi(line);
    parse_line(f, &line, &linesize);
    settings.nparity = atoi(line);
    parse_line(f, &line, &linesize);
    settings.ngroup = atoi(line);

    settings.peers = malloc(sizeof(struct sockaddr) * (settings.nshard + settings.nparity));
    struct sockaddr_in *peers = (struct sockaddr_in *)settings.peers;

    // parse group details
    int need_parsed_lid = settings.nshard+settings.nparity;

    for (;need_parsed_lid;) {
        parse_line(f, &line, &linesize);
        int gid, nid, lid, port;
        char ip[128];
        sscanf(line, " %d %d %d %s %d", &nid, &gid, &lid, ip, &port);
        if (gid == settings.gid) {
            // same group
            peers[lid].sin_family = AF_INET;
            peers[lid].sin_port = htons(port);
            peers[lid].sin_addr.s_addr = inet_addr(ip);
            bzero(peers[lid].sin_zero, 8);
            if (lid == settings.lid) {
                settings.port = port;
            }
            need_parsed_lid--;
        }
    }

    if (line) free(line);
    fclose(f);
}

static int cocytus_connect_to_peer(int i)
{
    struct addrinfo    ai;
    memset(&ai, 0, sizeof(ai));
    ai.ai_family   = AF_INET;
    ai.ai_socktype = SOCK_STREAM;

    debug("connect to %d\n", i);
    // create socket
    int s = new_socket(&ai);
    if (s == -1) {
        fprintf(stderr, "failed to create socket\n");
        return -1;
    }
    // change peer state
    peers[i].state = peer_connecting;
    conn *c = NULL;
    // connect
    if (connect(s, &settings.peers[i], sizeof(struct sockaddr_in)) == 0) {

        c = conn_new(s, conn_peer_connect, EV_WRITE | EV_PERSIST, DATA_BUFFER_SIZE, false, threads[0].base);
        if (c == NULL) {
            fprintf(stderr, "failed to create client conn");
            close(s);
            return -1;
        }
        peers[i].conn_w = c;
        //event_active(&c->event, EV_WRITE, 0);
        //drive_machine(c);
    } else {

        if (errno == EINPROGRESS) {
            debug("EINPROGRESS fd=%d\n",s);
            c = conn_new(s, conn_peer_connect, EV_WRITE | EV_PERSIST, DATA_BUFFER_SIZE, false, threads[0].base);

            if(c == NULL){
                fprintf(stderr, "connect_to_peer: failed to create client conn");
                close(s);
                return -1;
            } else {
                //c->thread =
            }
            peers[i].conn_w = c;

        } else {
            fprintf(stdout,"connect_to_peer: \n");
            close(s);
            return -1;
        }
    }
    return 0;
}

static int cocytus_connect_peers()
{
    int i;
    int npeer = settings.nparity + settings.nshard;
    int fd[2];

    // create and init peers
    peers = malloc(sizeof(struct peer) * (npeer));
    for (i=0; i<npeer; ++i) {
        //pthread_mutex_init(&peers[i].mutex, NULL);
        //pthread_mutex_lock(&peers[i].mutex);
        peers[i].conn_r = NULL;
        peers[i].conn_w = NULL;
        peers[i].state = peer_init;
        peers[i].rep_queue = NULL;
        peers[i].sub_by_or_as_lid = i;
        peers[i].done_xid = 0;
    }
    if (IS_PARITY_SERVER) {
        ecallocs = malloc(sizeof(struct ecalloc) * settings.nshard);
        assert(ecallocs);
        ecalloc = ecallocs;
        FOREACH_DATA_LID(i) {
            peers[i].rep_queue = malloc(sizeof(struct rep_queue));
            assert(peers[i].rep_queue);
            rep_queue_init(peers[i].rep_queue, 512);
            ecalloc_init(&ecallocs[i], MEMSIZE);

            peers[i].touch_flags = calloc(sizeof(char), MEMSIZE / UNITSIZE);
        }
    } else {
        ecalloc = ecallocs = ecmem.ecalloc;
    }

    // queue for the one who sends the rep
    rep_queue = malloc(sizeof(struct rep_queue));
    assert(rep_queue);
    rep_queue_init(rep_queue, 512);

    debug("connect to all lid > mylid\n");
    for (i=settings.lid+1; i<npeer; ++i) {
        cocytus_connect_to_peer(i);
    }

    if (pipe(fd)==-1) {
        perror("failed to pipe!\n");
        exit(-1);
    }
    //peers[settings.lid].conn_r = conn_new(fd[0], conn_new_cmd, EV_READ | EV_PERSIST, DATA_BUFFER_SIZE, false, threads[0].base);
    //peers[settings.lid].conn_w = conn_new(fd[1], conn_write, EV_PERSIST, DATA_BUFFER_SIZE, false, threads[0].base);
    if (IS_PARITY_SERVER) {
        idle_recovery_event = event_new(threads[0].base,
                fd[1], EV_PERSIST, idle_event_handler, NULL);
        event_priority_set(idle_recovery_event, 3);
        event_add(idle_recovery_event, 0);
    } else {
        idle_recovery_event = NULL;
    }

    return 0;
}

static void cocytus_init() {

    if (IS_PARITY_SERVER) {
        settings.port = ntohs((
                    (struct sockaddr_in *)&settings.peers[settings.lid])->sin_port);
    }

    peers_read_ready = 0;
    peers_write_ready = 0;
    // < alloc_xid is alloced
    alloc_xid = 1;
    // <= stable/updated_xid is stable/updated
    stable_xid = 0;
    updated_xid = 0;
    //pthread_mutex_init(&xid_mutex, NULL);
    //pthread_mutex_init(&peers_ready_mutex, NULL);

    recovery_leader = settings.nshard;
    sub_as_lid = settings.lid;
    sub_as_ready = 0;

    // init leader ring
    queue_init(&leader_ring, settings.nparity);
    int i;
    FOREACH_PARITY_LID(i) {
        queue_enqueue(&leader_ring, i);
    }

    cocytus_connect_peers();
}

#endif

#ifdef ENABLE_COCYTUS
// COCYTUS FUNCIONS

static int buf_set_u64(char *p, uint64_t n)
{
    char b[128];
    return p ? sprintf(p, "%" PRIu64, n):
               sprintf(b, "%" PRIu64, n);
}

static int buf_set_u32(char *p, uint32_t n)
{
    char b[128];
    return p ? sprintf(p, "%" PRIu32, n):
               sprintf(b, "%" PRIu32, n);
}

static int parity_send(conn *c, conn *target_c, uint64_t xid, item *it, void *diff)
{
    if (settings.verbose) {
        printf("replication_rep\n");
    }
    //int   r = 0;
    int exp = 0;
    int len = 0;
    char *s = "rep ";
    char *n = "\r\n";
    char *p = NULL;
    char flag[40];

    if (it->exptime)
        exp = it->exptime + stats.started;
    flag[0] = 0;
    if ((p=ITEM_suffix(it))) {
        int i;
        memcpy(flag, p, it->nsuffix - 2);
        flag[it->nsuffix - 2] = 0;
        for (i=0; i<strlen(flag); i++) {
            if (flag[i] > ' ')
                break;
        }
        memmove(flag,&flag[i],strlen(flag)-i);
        for (p=flag; *p>' '; p++);
        *p = 0;
    }
    // rep $key $flag $exp $nvalue $cas $lid $addr $conn_ptr $xid $stable_xid\r\n
    // $value\r\n
    len += strlen(s);
    len += it->nkey;
    len += 1;
    len += strlen(flag);
    len += 1;
    len += buf_set_u32(NULL, (uint32_t)exp);
    len += 1;
    len += buf_set_u32(NULL, (uint32_t)(it->nbytes - 2));
    len += 1;
    len += buf_set_u32(NULL, (uint32_t)it->data[0].cas);
    len += 1;
    len += buf_set_u32(NULL, (uint32_t)sub_as_lid);
    len += 1;
    len += buf_set_u64(NULL, (uint64_t)it->addr);
    len += 1;
    len += buf_set_u64(NULL, (uint64_t)target_c);
    len += 1;
    len += buf_set_u64(NULL, (uint64_t)xid);
    len += 1;
    len += buf_set_u64(NULL, (uint64_t)stable_xid);
    len += strlen(n);
    len += it->nbytes;
    len += strlen(n);
    if (conn_touch_buf(c,len) == -1) {
        fprintf(stderr, "replication: rep alloc error\n");
        return(-1);
    }
    p = c->wcurr + c->wbytes;
    memcpy(p, s, strlen(s));
    p += strlen(s);
    memcpy(p, ITEM_key(it), it->nkey);
    p += it->nkey;
    *(p++) = ' ';
    memcpy(p, flag, strlen(flag));
    p += strlen(flag);
    *(p++) = ' ';
    p += buf_set_u32(p, (uint32_t)exp);
    *(p++) = ' ';
    p += buf_set_u32(p, (uint32_t)(it->nbytes - 2));
    *(p++) = ' ';
    p += buf_set_u32(p, (uint32_t)it->data[0].cas);
    *(p++) = ' ';
    p += buf_set_u32(p, (uint32_t)sub_as_lid);
    *(p++) = ' ';
    p += buf_set_u64(p, (uint64_t)it->addr);
    *(p++) = ' ';
    p += buf_set_u64(p, (uint64_t)target_c);
    *(p++) = ' ';
    p += buf_set_u64(p, (uint64_t)xid);
    *(p++) = ' ';
    p += buf_set_u64(p, (uint64_t)stable_xid);
    memcpy(p, n, strlen(n));
    p += strlen(n);
    memcpy(p, diff, it->nbytes);
    p += it->nbytes;
    c->wbytes = p - c->wcurr;
    debug("target_c: %p\n", (void *)target_c);
    return(0);
}

static int conn_flush_wbuf(conn *c)
{
    int w;
    if (!c) return(0);
    if (c->wbytes == 0) {
        if (!update_event(c, EV_PERSIST)) {
            debug("case conn_waiting: could not update event\n");
            conn_set_state(c, conn_closing);
        }
        return 0;
    }

    while (c->wbytes) {
        w = write(c->sfd, c->wcurr, c->wbytes);
        if (w == -1) {

            if (errno == EINTR) {
                continue;
            }
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
                //FIXME
                return c->wbytes;
            }
            debug("conn_flush_wbuf: send error: %s\n", strerror(errno));
            //FIXME
            //replication_close();
            conn_set_state(c, conn_closing);
            return -1;
        }
        c->wbytes -= w;
        c->wcurr  += w;
        debug("conn_flus_wbuf, %d\n", w);
    }
    c->wcurr = c->wbuf;
    return 0;
}

static int conn_touch_buf(conn *c, int s)
{
    char *p;
    s += c->wbytes;
    s += c->wcurr - c->wbuf;
    if (c->wsize < s) {
        while (c->wsize < s) {
            c->wsize += DATA_BUFFER_SIZE;
        }
        if (c->wsize > MAX_SENDBUF_SIZE) {
            fprintf(stderr, "conn_buf_touch: alloc error: wsize over MAX_SENDBUF_SIZE\n");
            return -1;
        }
        if (!(p = malloc(c->wsize))) {
            fprintf(stderr, "conn_buf_touch: alloc error: %s\n", strerror(errno));
            return -1;
        }
        memcpy(p, c->wcurr, c->wbytes);
        free(c->wbuf);
        c->wbuf = p;
        c->wcurr = p;
    }
    return 0;
}

static int send_msgbuf_raw(conn *c, char *buf, int nbuf)
{
    //char *n = "\r\n";
    char *p = NULL;

    //int len = nbuf + 2;
    int len = nbuf;
    if (conn_touch_buf(c, len) == -1) {
        fprintf(stderr, ": prealloc error\n");
        return -1;
    }

    p = c->wcurr + c->wbytes;
    memcpy(p, buf, nbuf);
    p += nbuf;
    //memcpy(p, n, strlen(n));
    //p += strlen(n);

    assert(p - c->wcurr == c->wbytes + len);

    c->wbytes = p - c->wcurr;
    return 0;
}

static int vsend_msgf_raw(conn *c, const char *fmt, va_list ap)
{
    int len = 0;
    char *n = "\r\n";
    char *p = NULL;

    //va_list ap;
    //va_start(ap, fmt);
    char buf[512];
    int nbuf;
    nbuf = len = vsnprintf(buf, 511, fmt, ap);
    //va_end(ap);
    if (len > 511) {
        debug("msg too long\n");
        exit(-1);
    }
    debug("send_msgf: %s\n", buf);
    len += strlen(n);
    if (conn_touch_buf(c, len) == -1) {
        fprintf(stderr, ": prealloc error\n");
        return -1;
    }
    //printf("-1 wcurr:%p wbytes:%d \n", (void *)c->wcurr, c->wbytes);
    p = c->wcurr + c->wbytes;
    //printf("0 wcurr:%p wbytes:%d \n", (void *)c->wcurr, c->wbytes);
    //printf("0:%p\n", (void *)p);
    memcpy(p, buf, nbuf);
    p += nbuf;
    //printf("1 wcurr:%p wbytes:%d \n", (void *)c->wcurr, c->wbytes);
    //printf("1:%p\n", (void *)p);
    memcpy(p, n, strlen(n));
    p += strlen(n);
    //printf("2 wcurr:%p wbytes:%d \n", (void *)c->wcurr, c->wbytes);
    //printf("2:%p\n", (void *)p);

    if (p - c->wcurr != c->wbytes + len) {
        printf("%s\n", buf);
        printf("%d\n", nbuf);
        printf("%d\n", (int)strlen(n));
        printf("%p %p %lld %d\n", (void *)p, (void *)c->wcurr, (long long)c->wbytes, len);
        printf("%s\n", c->wcurr);
    }
    assert(p - c->wcurr == c->wbytes + len);

    c->wbytes = p - c->wcurr;
    return 0;
}

static int send_msgf_raw(conn *c, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int r = vsend_msgf_raw(c, fmt, ap);
    va_end(ap);
    return r;
}

static int send_msgf(conn *c, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int r = vsend_msgf_raw(c, fmt, ap);
    va_end(ap);

    int lid = get_real_peer_lid_by_conn(c);
    if (peers[lid].state == peer_connected) {
        conn_set_state(c, conn_write_to_peer);
        event_active(&c->event, EV_WRITE, 0);
    }
    //c->write_and_go = conn_new_cmd;
    return r;
}

static inline int is_peer_conn(conn *c) {
    int i;
    if (!IS_PARITY_SERVER) {
        for (i=0; i<settings.nparity; ++i) {
            if (peers[i].conn_r == c) {
                //debug("==parity conn closing =====\n");
                return 2;
            }
        }
    } else {
        for (i=0; i<settings.nshard; ++i) {
            if (peers[i].conn_r == c) {
                //debug("==shard conn closing =====\n");
                return 1;
            }
        }
    }
    return 0;
}

static void queue_rep_command(conn *c, token_t *tokens, const size_t ntoken,
        int comm, bool handle_cas)
{
    char *key;
    size_t nkey;
    unsigned int flags;
    int32_t exptime_int = 0;
    time_t exptime;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    assert(c != NULL);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (! (safe_strtoul(tokens[2].value, (uint32_t *)&flags)
           && safe_strtol(tokens[3].value, &exptime_int)
           && safe_strtol(tokens[4].value, (int32_t *)&vlen))) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
    exptime = exptime_int;

    /* Negative exptimes can underflow and end up immortal. realtime() will
       immediately expire values that are greater than REALTIME_MAXDELTA, but less
       than process_started, so lets aim for that. */
    if (exptime < 0)
        exptime = REALTIME_MAXDELTA + 1;

    // does cas value exist?
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    int lid;
    safe_strtol(tokens[6].value, (int32_t *)&lid);
    assert(lid == get_peer_lid_by_conn(c));
    struct rep_queue_item *e = rep_queue_add(peers[lid].rep_queue);
    e->done = 0;
    c->lid = e->lid = (int32_t)lid;
    safe_strtoull(tokens[7].value, (uint64_t *)&e->addr);
    safe_strtoull(tokens[8].value, (uint64_t *)&e->conn_ptr);
    safe_strtoul(tokens[9].value, (uint32_t *)&e->xid);
    debug("got rep %s %s %s \n", key, tokens[8].value, tokens[9].value);
    debug("queue_rep: lid %d xid %d\n", (int)lid, (int)e->xid);
    c->xid = e->xid;
    switch_hashtable(lid);

    vlen += 2;
    if (vlen < 0 || vlen - 2 < 0) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, flags, realtime(exptime), vlen);

    if (it == 0) {
        if (! item_size_ok(nkey, flags, vlen))
            out_string(c, "SERVER_ERROR object too large for cache");
        else
            out_of_memory(c, "SERVER_ERROR out of memory storing object");
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (comm == NREAD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        return;
    }

    ADDR addr = e->addr;
    ADDR ret;
    if (get_real_peer_lid_by_conn(c) != lid) {
        int check = ec_check(&ecallocs[lid], addr, it->nbytes);
        debug("check=%d\n", check);
        if (check) {
            ret = ec_alloc(&ecallocs[lid], it->nbytes);
            debug(BG_G"ec_alloc get %"PRIu64" in queue_rep_cmd\n"BG_D, ret);
            assert(ret == addr);
        } else {
            ret = pac_queue_pop(get_real_peer_lid_by_conn(c));
            assert(ret == addr);
        }
    } else {
        ret = ec_alloc(&ecallocs[lid], it->nbytes);
        debug(BG_G"ec_alloc get %"PRIu64" in queue_rep_cmd\n"BG_D, ret);
    }
    ret = ret;
    debug("%"PRIu64" vs. %"PRIu64"\n", addr, ret);
    assert(ret == addr);

    it->addr = e->addr;
    it->nbytes = vlen;
    ITEM_set_cas(it, req_cas_id);

    e->cmd = comm;
    c->item = 0;
    e->item = it;

    if (e->vnbytes < it->nbytes || (e->vnbytes >> 1) > it->nbytes) {
        if (e->vbuf) free(e->vbuf);
        e->vbuf = malloc(it->nbytes);
        e->vnbytes = it->nbytes;
    }

    c->ritem = e->vbuf;
    c->rlbytes = it->nbytes;
    c->cmd = comm;
    conn_set_state(c, conn_nread);
}

static void process_rep_command(conn *c, uint64_t xid, int deadlid)
{
    //assert(c != NULL);
    int lid;
    if (c == NULL) lid = deadlid;
    else lid = get_peer_lid_by_conn(c);

    debug("lid %d\n", lid);
    struct rep_queue_item *e = rep_queue_find(peers[lid].rep_queue, xid);
    assert(e != NULL);
    assert(e->xid == xid);
    item *it = e->item;
    debug("process_rep: lid %d xid %d\n", (int)lid, (int)xid);

    // is parity server
    uint64_t addr = e->addr;
    assert(lid == e->lid);


    int needupdate = recovery_try_update_unit(
            &recovery, lid, addr, e->vbuf, it->nbytes);

    //assert(e->vnbytes == it->nbytes);
    if (needupdate) {
        char *addrp = ecmem_get(&ecmem, addr);
        galois_w08_region_multiply(e->vbuf,
            MATRIX(settings.lid, lid), it->nbytes,
            addrp, 1);
    }
    //
    switch_hashtable(lid);

    // ===========================
    // complete_nread_ascii(conn)
    // ===========================
    assert(e != NULL);

    int comm = e->cmd;
    //enum store_item_type ret;

    // XXX
    if (0 && strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) != 0) {
        // FIXME: maybe concurrency bugs of c->wbuf
        out_string(c, "CLIENT_ERROR bad data chunk");
    } else {
        // FIXME return value not checked
        store_item(it, comm, c);
    }

    if (c) conn_set_state(c, conn_new_cmd);

    item_remove(e->item);       /* release the c->item reference */
    e->item = 0;
    e->done = 1;

    debug("process finished\n");

    rep_queue_flush(peers[lid].rep_queue);
    debug("queue flushed\n");
}

static int get_real_peer_lid_by_conn(conn *c)
{
    int i;
    int npeer = settings.nparity + settings.nshard;
    for (i=0; i<npeer; ++i) {
        if (peers[i].conn_r == c) return i;
        if (peers[i].conn_w == c) return i;
    }
    return -1;
}

static int get_peer_lid_by_conn(conn *c)
{
    int i;
    int npeer = settings.nparity + settings.nshard;
    for (i=0; i<npeer; ++i) {
        if (peers[i].conn_r == c) return peers[i].sub_by_or_as_lid;
        if (peers[i].conn_w == c) return peers[i].sub_by_or_as_lid;
    }
    return -1;
}

void
send_recovered_data(int leader, int unit_begin, int unit_end, uint32_t mask)
{
    send_msgf_raw(peers[leader].conn_w,
            "recover_units_gather %d %d %d %d %"PRIu32,
            leader, settings.lid, unit_begin, unit_end, mask);
    int i;
    struct recovery *r = &recovery;
    for (i=unit_begin; i<=unit_end; ++i) {
        struct recovery_unit *unit = &r->units[i];
        send_msgbuf_raw(peers[leader].conn_w, unit->data, UNITSIZE);
    }

    if (peers[leader].state == peer_connected) {
        conn_set_state(peers[leader].conn_w, conn_write_to_peer);
        event_active(&peers[leader].conn_w->event, EV_WRITE, 0);
    }
}

void
complete_recovery_bottom_half(struct recovery_queue_item *rqit)
{
    int n = 0;
    int nn = 0;
    int m = 0;
    int i, j;
    // count for lost peers
    for (i=0; i<settings.nshard; ++i) {
        //if (peers[i].state == peer_lost) n++;
        if ((rqit->mask & (1<<i)) == 0) n++;
    }

    struct recovery_queue_item *it = rqit;
    // copy self party data back to it->data_from_parity
    //
    struct recovery *r = &recovery;
    int nbuf = (it->unit_end - it->unit_begin + 1) * UNITSIZE;
    //char *buf = ecmem_get(sub_ecmem, it->unit_begin * r->unit_size);
    char *buf = NULL;
    if (it->mask & (1<<settings.lid)) {
        buf = malloc(nbuf);
        assert(buf);
        char *p = buf;
        for (i=it->unit_begin; i<=it->unit_end; ++i) {
            struct recovery_unit *unit = &r->units[i];
            memcpy(p, unit->data, UNITSIZE);
            p += UNITSIZE;
        }
        p = NULL;
    }

    // tmp mat
    int *tmpmat = (int *)malloc(sizeof(int)*n*n);
    assert(tmpmat);

    // fill in the tmp mat
    char **C = malloc(sizeof(char *) * n);
    assert(C);
    uint32_t mask = it->mask;
    for (i=settings.nshard; i<settings.nparity+settings.nshard; ++i) {
        if (mask & (1<<i)) {
            if (i == settings.lid) {
                C[m++] = buf;
                buf = NULL;
            } else {
                C[m++] = it->data_from_parity[i];
            }
            for (j=0; j<settings.nshard; ++j) {
                if ((mask & (1<<j)) == 0) {
                    tmpmat[nn++] = MATRIX(i, j);
                }
            }
        }
    }
    if (nn != n * n) {
        debug("nn= %d  n=%d\n", nn, n);
    }
    assert(nn == n * n);
    assert(m == n);

    // inverted mat
    int *invmat = (int *)malloc(sizeof(int)*n*n);
    assert(invmat);

    debug(" invert matrix calc: n=%d ", n);
    m = jerasure_invert_matrix(tmpmat, invmat, n, 8);
    assert(m == 0);
    free(tmpmat); tmpmat = NULL;

    char **data = malloc(sizeof(char *) * n);
    for (i=0; i<n; ++i) {
        data[i] = calloc(sizeof(char), nbuf);
    }

    for (i=0; i<n; ++i) {
        for (j=0; j<n; ++j) {
            galois_w08_region_multiply(C[j],
                    invmat[i*n+j], nbuf, data[i], 1);
            //d[i] += cj * invmat[i*n+j];
        }
    }

    // free memory
    free(invmat); invmat = NULL;
    for (i=0; i<n; ++i) {
        free(C[i]);
    }
    free(C); C = NULL;
    free(it->data_from_parity); it->data_from_parity = NULL;

    int x = 0;

    FOREACH_PARITY_LID(i) {
        if (mask & (1<<i)) {
            int who = i;
            if (who == settings.lid) {
                debug("fill my recovered data\n");
                fill_completed_recovered_data(
                        it->unit_begin,
                        it->unit_end,
                        data[x++]);
            } else {
                // send completely recovered data back
                send_msgf_raw(peers[who].conn_w,
                        "recover_units_scatter %d %d %"PRIu32" %"PRIu64,
                        it->unit_begin, it->unit_end, it->mask,
                        who == it->req_lid ? it->conn : 0);

                send_msgbuf_raw(peers[who].conn_w, data[x++], nbuf);

                if (peers[who].state == peer_connected) {
                    conn_set_state(peers[who].conn_w, conn_write_to_peer);
                }
            }
        }
    }
    assert(n == x);
    for (i=0; i<n; ++i) {
        free(data[i]);
    }
    free(data); data = NULL;
}

void *ecmem_unmapper(void *v);

void
fill_completed_recovered_data(int ubegin, int uend, char *data)
{
    int i;
    i = ubegin;
    uint64_t old = sub_finished_recovery_count;
    sub_finished_recovery_count += uend - ubegin + 1;
    if (sub_finished_recovery_count * 100 / sub_init_need_recovery_count
            != old * 100 / sub_init_need_recovery_count) {

       int percent = sub_finished_recovery_count * 100 / sub_init_need_recovery_count;
       printf("[recovery progress] %2d precent (%llu in %llu)\n",
               percent, (unsigned long long)sub_finished_recovery_count, (unsigned long long)sub_init_need_recovery_count);

    }

    // check
    while (i <= uend) {
        // don't recover those flags=2
        while (i<=uend && sub_flags[i]==2) i++;
        if (i > uend) break;

        int start = i;
        while (i<=uend && sub_flags[i]!=2) {
            sub_flags[i] = 2;
            i++;
        }
        // fill in the data
        // start  i-1
        char *addr = ecmem_get(sub_ecmem, start * UNITSIZE);
        int size = UNITSIZE * (i-1 - start+ 1);
        memcpy(addr, data + (start - ubegin) * UNITSIZE, size);

        bop_queue_invoke(start, i-1);
    }
    while (recovery_finished_unit < end_unit &&
           sub_flags[recovery_finished_unit]==2)
        recovery_finished_unit++;
    if (recovery_finished_unit == end_unit) {
        //pthread_t thread;
        //pthread_create(&thread, NULL, ecmem_unmapper, NULL);
    }

}

void *ecmem_unmapper(void *v)
{
    munmap(ecmem.mem, MEMSIZE);
    return NULL;
}

void
restart_failed_recovery(int failed_lid)
{
    struct recovery *r = &recovery;
    struct recovery_queue *rq = &r->queue;
    recovery_req_clean(r);
    int i;
    int entry = rq->entry;
    for (i=rq->exit; i<entry; ++i) {
        struct recovery_queue_item *it = &rq->items[i % rq->cap];
        if (it->mask & (1<<failed_lid)) {
            // this item needs to restart
            // NOTICE: flag in_use as 0 only when the other reply is received
            it->in_use = -1;
            int j;
            int maskie = (1<<settings.nshard)-1;
            for (j=it->unit_begin; j<=it->unit_end; ++j) {
                if ((it->mask & maskie) ==
                        ((r->units[j].flags & maskie) | (1<<failed_lid))) {
                    it->in_use = 0;
                }
                r->units[j].flags = 0;
                if (r->units[j].data) free(r->units[j].data);
                r->units[j].data = NULL;
            }
            start_recovery(it->unit_begin, it->unit_end, it->req_lid, it->conn);
        }
    }
}

void
shrink_recovery_interval(int *ubegin, int *uend)
{
    while (*ubegin <= *uend) {
        if ( sub_flags[*ubegin] == 0 ) break;
        *ubegin += 1;
    }
    while (*ubegin <= *uend) {
        if ( sub_flags[*uend] == 0 ) break;
        *uend -= 1;
    }
}

void
process_queued_items(int deadlid, int maxxid)
{
    struct rep_queue *queue = peers[deadlid].rep_queue;
    switch_hashtable(deadlid);
    rep_queue_clean(queue, maxxid);

    while (peers[deadlid].done_xid < maxxid) {
        process_rep_command(NULL, ++peers[deadlid].done_xid, deadlid);
    }
}

static void
start_fast_recovery(int ubegin, int uend, int req_lid, conn *c)
{
    // i am the recovery leader
    assert(recovery_leader == settings.lid);
    int i;
    for (i=ubegin; i<=uend; ++i) {
        assert(sub_flags[i] == 2);
    }
    // calc the peers involved in this recovery (the requester is always in)
    // leader is not included here for the pairty for subee of leader can
    // already been freed
    int remaining = settings.nshard - 2;
    uint32_t mask = (1<<sub_as_lid) | (1<<req_lid);
    // add others into mask
    for (i=0; i<NPEER && remaining; ++i) {
        if (i == settings.lid) continue;
        if (peers[i].state != peer_connected) continue;
        mask |= 1 << i;
        remaining--;
    }
    if (remaining) {
        printf("not enough to recover! remaining %d\n", remaining);
        assert(0);
    }

    struct recovery_queue_item *rqit = recovery_req_add(
            &recovery, req_lid, c, settings.lid, ubegin, uend, mask);
    rqit = rqit;
    assert(rqit);

    // send recover request to data peer
    for (i=0; i<settings.nshard; ++i) {
        if (peers[i].state != peer_connected) continue;
        if ((mask & (1<<i)) == 0) continue; // peer not involved
        if (i == settings.lid) continue; // don't send to myself

        // recover_units $leader_lid $unit_begin $unit_end $mask
        send_msgf(peers[i].conn_w, "recover_units %d %d %d %"PRIu32,
                settings.lid, ubegin, uend, mask);
    }
    FOREACH_PARITY_LID(i) {
        if (i == settings.lid) continue;
        if (mask & (1<<i)) {
            send_msgf_raw(peers[i].conn_w,
                    "recover_units_reply %d %d %d %"PRIu32" %"PRIu64,
                    settings.lid, ubegin, uend, mask, stable_xid);
            send_msgbuf_raw(peers[i].conn_w,
                    ecmem_get(sub_ecmem, ubegin * UNITSIZE),
                    (uend - ubegin + 1) * UNITSIZE);
            if (peers[i].state == peer_connected) {
                conn_set_state(peers[i].conn_w, conn_write_to_peer);
                event_active(&peers[i].conn_w->event, EV_WRITE, 0);
            }
        }
    }
    // apply sub's data to my recover
    //recovery_recover_units(&recovery, &ecmem, sub_as_lid,
    //        ubegin, uend, ecmem_get(sub_ecmem, ubegin * UNITSIZE));
}


static void
start_recovery(int ubegin, int uend, int req_lid, conn *c)
{
    // i am the recovery leader
    assert(recovery_leader == settings.lid);

    // calc the peers involved in this recovery (leader is always in)
    int remaining = settings.nshard - 1;
    uint32_t mask = 1<<(settings.lid);
    // add others into mask
    int i;
    for (i=0; i<settings.nshard+settings.nparity && remaining; ++i) {
        if (i == settings.lid) continue;
        if (peers[i].state != peer_connected) continue;
        mask |= 1 << i;
        remaining--;
    }
    if (remaining) {
        printf("not enough to recover!\n");
        assert(0);
    }

    struct recovery_queue_item *rqit = recovery_req_add(
            &recovery, req_lid, c, settings.lid, ubegin, uend, mask);
    rqit = rqit;
    assert(rqit);

    // send recover request to data peer
    for (i=0; i<settings.nshard; ++i) {
        //if (peers[i].state != peer_connected) continue; // already checked
        if ((mask & (1<<i)) == 0) continue; // peer not involved
        if (i == settings.lid) continue; // don't send to myself

        // recover_units $leader_lid $unit_begin $unit_end $mask
        send_msgf(peers[i].conn_w, "recover_units %d %d %d %"PRIu32,
                settings.lid, ubegin, uend, mask);
    }
}

static void
do_recovery(conn *c, int ubegin, int uend)
{
    assert(ubegin <= uend);
    int i;
    for (i=ubegin; i<=uend; ++i) {
        assert(sub_flags[i] == 0);
        sub_flags[i] = 1;
    }

    if (recovery_leader == settings.lid) {
        // i am the leader
        start_recovery(ubegin, uend, settings.lid, c);
    } else {
        // send to recovery leader // $req_lid can be obtained from the conn
        // recover_req $ubegin $uend $conn_ptr
        send_msgf(peers[recovery_leader].conn_w, "recover_req %d %d %"PRIu64,
                ubegin, uend, (uint64_t)c);
    }
}


static int
need_recovery(int ubegin, int uend)
{
    int nwait = 0;
    int i = ubegin;
    while (i<=uend) {
        while (i<=uend && sub_flags[i] != 0) {
            if (sub_flags[i] != 2) nwait++;
            i++;
        }
        if (i>uend) break;
        return 1;
    }
    return nwait;
}

static int
try_do_recovery(conn *c, int ubegin, int uend, enum conn_states state_to_go)
{
    int nwait = 0;
    int i = ubegin;
    while (i<=uend) {
        // don't need to recover those flags=1, which will be retried
        // don't need to recover those flags=2, which is already been recovered
        while (i<=uend && sub_flags[i] != 0) {
            if (sub_flags[i] != 2) nwait++;
            i++;
        }
        if (i>uend) break;
        int start = i;
        // only add those flags=0
        while (i<=uend && sub_flags[i] == 0) {
            nwait++;
            i++;
        }
        do_recovery(c, start, i-1);
    }

//    while (ubegin <= uend && sub_flags[ubegin] == 2) { ubegin++; }
//    while (ubegin <= uend && sub_flags[uend] == 2) { uend--; }
//    nwait = uend - ubegin + 1;
   // if (c == NULL) return nwait;
    if (nwait == 0) {
        conn_set_state(c, state_to_go);
    } else {
        //do_recovery(c, ubegin, uend);
        c->next_state = state_to_go;
        conn_set_state(c, conn_pre_waiting_ack);
        struct bop_queue_item *bopqit = bop_queue_add(ubegin, uend, nwait, c);
        bopqit = bopqit;
        assert(bopqit);
    }
    return nwait;
}

static void
assert_data_availability(uint64_t addr, int size)
{
    debug("checking data avialability\n");
    int ubegin = addr / UNITSIZE;
    int uend = (addr + size - 1) / UNITSIZE;
    int i;
    for (i=ubegin; i<=uend; ++i) {
        assert(sub_flags[i] == 2);
    }
}

static void
sub_for(int subee)
{
    debug("sub for %d", subee);

    sub_ecmem = malloc(sizeof(struct ecmem));
    sub_flags = calloc(sizeof(char), MEMSIZE / UNITSIZE);

    assert(sub_flags);
    assert(sub_ecmem);
    ecmem_init(sub_ecmem, MEMSIZE);

    sub_as_lid = -subee;
    sub_as_ready = 0;
    stable_nxid = settings.nparity - 1;
    stable_xid = rep_queue_maxxid(peers[subee].rep_queue, peers[subee].done_xid);

    debug("my stable_xid %"PRIu64"\n", stable_xid);

    int i;
    FOREACH_PARITY_LID(i) {
        // subpeer $gid $leaderlid $deadlid
        if (i == settings.lid) continue;
        if (peers[i].state == peer_connected) {
            send_msgf(peers[i].conn_w, "subpeer %d %d %d",
                    settings.gid, settings.lid, subee);
        } else stable_nxid--;
    }
    if (stable_nxid == 0) {
        alloc_xid = stable_xid + 1;
        sub_as_lid = -sub_as_lid;
        sub_as_ready = 1;
        process_queued_items(sub_as_lid, stable_xid);
        //
        int ucount = MEMSIZE / UNITSIZE;
        sub_init_need_recovery_count = 0;
        sub_finished_recovery_count = 0;
        end_unit = 0;
        for (i=0; i<ucount; ++i) {
            sub_flags[i] = peers[sub_as_lid].touch_flags[i] ? 0 : 2;
	    if (sub_flags[i] == 0) end_unit = i+1;
            sub_init_need_recovery_count += peers[sub_as_lid].touch_flags[i];
        }
        update_idle_event(idle_recovery_event, EV_WRITE | EV_PERSIST);
    }
}

#endif
