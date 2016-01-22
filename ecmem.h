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
#ifndef ECMEM_H
#define ECMEM_H

#include <sys/mman.h>
#include "ecalloc.h"
#include "def.h"

struct ecmem {
	uint64_t size;
	void *mem;
	struct ecalloc *ecalloc;
};

static inline void ecmem_init(struct ecmem *ecmem, uint64_t size)
{
	size = (size + 4096 - 1) >> 12 << 12;
	ecmem->size = size;
	ecmem->mem = mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, 0, 0);
	ecmem->ecalloc = safe_malloc(sizeof(struct ecalloc));
	ecalloc_init(ecmem->ecalloc, size);
}

static inline ADDR ecmem_alloc(struct ecmem *ecmem, uint64_t size)
{
	return ec_alloc(ecmem->ecalloc, size);
}

static inline void ecmem_free(struct ecmem *ecmem, ADDR addr)
{
	ec_free(ecmem->ecalloc, addr);
}

static inline void *ecmem_get(struct ecmem *ecmem, ADDR addr)
{
	return (char *)ecmem->mem + addr;
}

#endif
