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
#ifndef ECALLOC_H
#define ECALLOC_H

#include <assert.h>
#include <pthread.h>

#include "avltree.h"

#define USING 0x1

typedef uint64_t ADDR;

struct ecalloc {
	struct avltree free_tree; /* sorted by size */
	struct avltree used_tree; /* sorted by addr */
	uint64_t size;
	uint64_t used;
	pthread_mutex_t mutex;
};

struct ecalloc_header
{
	ADDR addr;
	uint64_t size;
	uint64_t flag; /* to know whether this header is used */
	struct ecalloc_header *next;
	struct ecalloc_header *prev;
	struct avlnode *node;
};

void ecalloc_init(struct ecalloc* ecalloc, uint64_t size);
ADDR ec_alloc(struct ecalloc *ecalloc, uint64_t size);
int ec_check(struct ecalloc *ecalloc, ADDR addr, uint64_t size);
void ec_free(struct ecalloc *ecalloc, ADDR addr);

#endif
