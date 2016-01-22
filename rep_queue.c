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
#include "rep_queue.h"
#include "ecalloc.h"
#include "memcached.h"

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
extern struct ecalloc *ecalloc;

/*
 * +------------+-----------+---------+
 * |            |---stuff---|         |
 * +------------+-----------+---------+
 * ^             ^           ^        ^
 * 0           tail        head      cap
 */
inline int
rep_queue_full(struct rep_queue *q)
{
    return q->head - q->tail == q->cap;
}

//inline int
//rep_queue_empty(struct rep_queue *q)
//{
//    return q->head == q->tail;
//}

struct rep_queue_item *
rep_queue_add(struct rep_queue *q)
{
    if (rep_queue_full(q)) return 0;
    if (q->tail > q->cap) {
        q->tail -= q->cap;
        q->head -= q->cap;
    }
    struct rep_queue_item *e = &q->items[q->head % q->cap];
    e->ack = 0;
    q->head++;
    return e;
}

int
rep_queue_init(struct rep_queue *q, uint32_t cap)
{
    q->cap = cap;
    q->head = 0;
    q->tail = 0;
    q->items = (struct rep_queue_item *)malloc(sizeof(struct rep_queue_item) * cap);
    assert(q->items);
    return 0;
}

struct rep_queue_item *
rep_queue_find(struct rep_queue *q, uint64_t xid)
{
    int i;
    for (i=q->tail; i!=q->head; ++i) {
        if (q->items[i % q->cap].xid == xid) return &q->items[i % q->cap];
    }
    return NULL;
}

void
rep_queue_flush(struct rep_queue *q)
{
    uint32_t *head = &q->head;
    uint32_t *tail = &q->tail;
    while (*head != *tail) {
        struct rep_queue_item *item = &q->items[*tail % q->cap];
        if (item->done) {
            *tail = (*tail + 1);
            item->done = 0;
            if (item->vbuf) {
                free(item->vbuf);
                item->vbuf = NULL;
                item->vnbytes = 0;
            }
        } else {
            break;
        }
    }
}

uint64_t
rep_queue_maxxid(struct rep_queue *q, uint64_t init)
{
    uint64_t max = init;
    int i;
    for (i=q->tail; i<q->head; ++i) {
        struct rep_queue_item *rqit = &q->items[i % q->cap];
        if (rqit->xid > max && rqit->ack) {
            max = rqit->xid;
        }
    }
    return max;
}

void
rep_queue_clean(struct rep_queue *q, uint64_t maxxid)
{
    // precondition:
    //  the items in the queue is asc-ordered by xid
    int i;
    if (q->tail == q->head) return;
    int newhead = q->head;
    for (i=q->tail; i<q->head; ++i) {
        struct rep_queue_item *item = &q->items[i % q->cap];
        uint64_t xid = item->xid;
        if (xid > maxxid) {
            debug(BG_R"maddr to ec_free %"PRIu64" in rep_queue_clean\n"BG_D, item->addr);
            ec_free(ecalloc, item->addr);
            item_remove(item->item);
            if (newhead == q->head) {
                newhead = i;
            }
        }
    }
    q->head = newhead;
}

struct rep_queue_item *
rep_queue_first(struct rep_queue *q)
{
    if (q->head == q->tail) return NULL;
    return &q->items[q->tail % q->cap];
}
