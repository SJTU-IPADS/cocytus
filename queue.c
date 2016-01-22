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
#include "queue.h"

#include <stdlib.h>

void
queue_init(struct queue *q, int size)
{
    q->cap = size;
    q->len = 0;
    q->array = malloc(sizeof(int) * q->cap);
}

int
queue_enqueue(struct queue *q, int e)
{
    if (q->len == q->cap) return -1;
    q->array[q->len] = e;
    q->len++;
    return 0;
}

int
quque_remove_and_dequeue(
        struct queue *q, int to_remove)
{
    if (q->len == 0) return -1;
    int r = q->array[0];
    int p = 0;
    int i;
    for (i=1; i<q->len; ++i) {
        if (q->array[i] == to_remove) continue;
        q->array[p++] = q->array[i];
    }
    q->len = p;
    return r;
}

int
queue_remove(struct queue *q, int to_remove)
{
    if (q->len == 0) return -1;
    int r = -1;
    int p = 0;
    int i;
    for (i=0; i<q->len; ++i) {
        if (q->array[i] == to_remove) {
            r = to_remove;
            continue;
        }
        q->array[p++] = q->array[i];
    }
    q->len = p;
    return r;
}

int
queue_dequeue(struct queue *q)
{
    if (q->len == 0) return -1;
    int r = q->array[0];
    int i;
    for (i=1; i<q->len; ++i) {
        q->array[i-1] = q->array[i];
    }
    q->len--;
    return r;
}

int
queue_find_first(struct queue *q)
{
    return q->array[0];
}
