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
// prealloc queue
#include <stdint.h>
struct pac_queue_item {
    uint64_t addr;
};

struct pac_queue {
    int exit;
    int entry;
    int cap;
    struct pac_queue_item *items;
};

void
pac_queues_init(int number);

struct pac_queue_item *
pac_queue_add(int idx, uint64_t addr);

uint64_t
pac_queue_pop(int idx);

int
pac_queue_size(int idx);
