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
// pre alloc queue
//
#include "pac_queue.h"
#include <stdlib.h>
#include <assert.h>

// pre alloc queue
struct pac_queue *pac_queues = NULL;

static void
pac_queue_init(struct pac_queue *pac)
{
    assert(pac);
    pac->exit = 0;
    pac->entry = 0;
    pac->cap = 128;
    pac->items = malloc(sizeof(struct pac_queue_item) * pac->cap);
}

void
pac_queues_init(int number)
{
    pac_queues = malloc(sizeof(struct pac_queue) * number);
    int i;
    for (i=0; i<number; ++i) {
        pac_queue_init(&pac_queues[i]);
    }
}


struct pac_queue_item *
pac_queue_add(int idx, uint64_t addr)
{
    struct pac_queue *pac = &pac_queues[idx];
    if (pac->entry - pac->exit >= pac->cap) return NULL;
    if (pac->exit > pac->cap) {
        pac->exit -= pac->cap;
        pac->entry -= pac->cap;
    }
    struct pac_queue_item *it = &pac->items[pac->entry % pac->cap];
    it->addr = addr;
    pac->entry++;
    return it;
}

uint64_t
pac_queue_pop(int idx)
{
    struct pac_queue *pac = &pac_queues[idx];
    struct pac_queue_item *it = &pac->items[pac->exit % pac->cap];
    pac->exit++;
    return it->addr;
}

int
pac_queue_size(int idx)
{
    struct pac_queue *pac = &pac_queues[idx];
    return pac->entry - pac->exit;
}
