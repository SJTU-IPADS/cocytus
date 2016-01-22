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
#ifndef REP_QUEUE_H
#define REP_QUEUE_H

#include <stdint.h>
#include "const.h"

struct rep_queue_item {
    uint64_t xid;
    int32_t lid;
    uint64_t addr;
    uint64_t conn_ptr;
    char *vbuf;
    int vnbytes;
    void *item;
    int cmd;
    int done;
    int ack;
};

struct rep_queue {
    struct rep_queue_item *items;
    uint32_t cap;
    uint32_t head;
    uint32_t tail;
    // [tail, head)
};

int rep_queue_full(struct rep_queue *q);

//inline int rep_queue_empty(struct rep_queue *q);

struct rep_queue_item *
rep_queue_add(struct rep_queue *q);

int
rep_queue_init(struct rep_queue *q, uint32_t cap);

struct rep_queue_item *
rep_queue_find(struct rep_queue *q, uint64_t xid);

void rep_queue_flush(struct rep_queue *q);

void
rep_queue_clean(struct rep_queue *q, uint64_t maxxid);

uint64_t
rep_queue_maxxid(struct rep_queue *q, uint64_t init);

struct rep_queue_item *
rep_queue_first(struct rep_queue *q);

#endif
