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
// blocked operation queue
//
#include "bop_queue.h"
#include <stdlib.h>
#include <assert.h>
#include "memcached.h"

#define MAX(a,b) (((a)>(b))?(a):(b))
#define MIN(a,b) (((a)<(b))?(a):(b))

struct bop_queue *bopq = NULL;

void
bop_queue_init(void)
{
    bopq = malloc(sizeof(struct bop_queue));
    assert(bopq);
    bopq->exit = 0;
    bopq->entry = 0;
    bopq->cap = 512;
    bopq->items = malloc(sizeof(struct bop_queue_item) * bopq->cap);
}

struct bop_queue_item *
bop_queue_add(int ubegin, int uend, int nwait, struct conn *conn)
{
    if (bopq->entry - bopq->exit >= bopq->cap) return NULL;
    if (bopq->exit > bopq->cap) {
        bopq->exit -= bopq->cap;
        bopq->entry -= bopq->cap;
    }
    struct bop_queue_item *it = &bopq->items[bopq->entry % bopq->cap];
    it->ubegin = ubegin;
    it->uend = uend;
    it->nwait = nwait;
    it->conn = conn;
    bopq->entry++;
    return it;
}

void
bop_queue_invoke(int ubegin, int uend)
{
    int i;
    for (i=bopq->exit; i<bopq->entry; ++i) {
        struct bop_queue_item *it = &bopq->items[i % bopq->cap];
        int left = MAX(ubegin, it->ubegin);
        int right = MIN(uend, it->uend);
        if (left > right) continue;
        it->nwait -= (right - left + 1);
        if (it->nwait == 0) {
            //event_active(&it->conn->event, EV_WRITE, 0);
        }
    }
    bop_queue_clean();
}

void
bop_queue_clean(void)
{
    int *exit = &bopq->exit;
    int *entry = &bopq->entry;
    while (*exit < *entry) {
        struct bop_queue_item *it = &bopq->items[*exit % bopq->cap];
        if (it->nwait == 0) {
            *exit += 1;
            event_active(&it->conn->event, EV_WRITE, 0);
            it->nwait = 1;
        } else break;
    }
}
