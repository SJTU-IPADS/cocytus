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
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include "const.h"


#include "ecalloc.h"

static void move_item(void *item, struct avlnode *src, struct avlnode *des)
{
	((struct ecalloc_header *)item)->node = des;
	return;
}

static int32_t addr_compare(void *l, void *r)
{
	uint64_t left = ((struct ecalloc_header *)l)->addr;
	uint64_t right = ((struct ecalloc_header *)r)->addr;

	if (left < right) {
		return -1;
	} else if (left == right) {
		return 0;
	} else {
		return 1;
	}
}

static int32_t size_compare(void *l, void *r)
{
	uint64_t left = ((struct ecalloc_header*)l)->size;
	uint64_t right = ((struct ecalloc_header *)r)->size;

	if (left < right) {
		return -1;
	} else if (left == right) {
		return 0;
	} else {
		return 1;
	}
}

void ecalloc_init(struct ecalloc *ecalloc, uint64_t size)
{
	struct ecalloc_header *header;
	avl_init(&ecalloc->free_tree, size_compare, move_item);
	avl_init(&ecalloc->used_tree, addr_compare, move_item);
	ecalloc->size = size;
	ecalloc->used = 0;
	header = safe_malloc(sizeof(struct ecalloc_header));
	header->addr = 0;
	header->size = size;
	header->flag = 0;
	header->prev = NULL;
	header->next = NULL;
	header->node = avl_insert(&ecalloc->free_tree, header);
	pthread_mutex_init(&ecalloc->mutex, NULL);
}

void ec_free(struct ecalloc *ecalloc, ADDR addr)
{
	struct ecalloc_header *header;
	struct ecalloc_header tmp;

	pthread_mutex_lock(&ecalloc->mutex);

	// find in used tree, free the node
	tmp.addr = addr;
	header = avl_remove(&ecalloc->used_tree, (void *)&tmp);

	if (!header) {
		printf("free a wrong addr %ld\n", addr);
		exit(-1);
	} else {
		ecalloc->used -= header->size;
		header->flag &= ~USING;
	}

	/* Combine neighbor behind */
	while (1) {
		struct ecalloc_header *next = header->next;

		/* If the next is in using, break */
		if (!next || next->flag & USING) break;
		assert(next->addr == header->addr + header->size);

		/* combine the two */
		header->size += next->size;
		header->next = next->next;
		if (header->next)
			header->next->prev = header;

		/* clean the next */
		avl_remove_node(&ecalloc->free_tree, next->node);
		free(next);
	}

	/* Combine neighbor front */
	while (1) {
		struct ecalloc_header *prev = header->prev;

		/* If the next is in using, break */
		if (!prev || prev->flag & USING) break;
		assert(prev->addr + prev->size == header->addr);

		/* combine the two */
		header->size += prev->size;
		header->addr = prev->addr;
		header->prev = prev->prev;
		if (header->prev)
			header->prev->next = header;

		/* clean the next */
		avl_remove_node(&ecalloc->free_tree, prev->node);
		free(prev);
	}

	header->node = avl_insert(&ecalloc->free_tree, header);
    pthread_mutex_unlock(&ecalloc->mutex);
}

// return 0 if check is passed
int
ec_check(struct ecalloc *ecalloc, ADDR addr, uint64_t size)
{
	struct avlnode *node;
	struct ecalloc_header *header;
	struct ecalloc_header tmp;

	tmp.addr = addr;

	node = avl_search(&ecalloc->used_tree, (void *)&tmp);
	if (!node) return -1;
	header = node->item;

	if (!header) {
		return -1;
	} else {
		if ((header->flag & USING) == 0) return -2;
		//if (header->size != size) return -3;
	}

    return 0;
}

ADDR ec_alloc(struct ecalloc *ecalloc, uint64_t size)
{
	ADDR ret;
	struct avlnode *node;
	struct ecalloc_header *header, *newer;
	struct ecalloc_header tmp;
	pthread_mutex_lock(&ecalloc->mutex);

	size = (size + 15) & (~15);
	ret = -1;

	/**
	 * Find the node containing enough space.
	 */
	tmp.size = size;
	node = avl_search_close(&ecalloc->free_tree, (void *)&tmp);
	if (!node) {
		perror("ec_alloc has no enough space");
		exit(-1);
	}
	header = (struct ecalloc_header *)node->item;
	if (header->size < size && node->next) {
		node = node->next;
		header = (struct ecalloc_header *)node->item;
	}
	if (header->size < size) {
		perror("ec_alloc has no enough space");
		exit(-1);
	} else {
		ret = header->addr;
		ecalloc->used += size;
	}

	/**
	 * Split the node into used and free
	 */
	avl_remove_node(&ecalloc->free_tree, node);
	if (header->size > size) {
		newer = safe_malloc(sizeof(struct ecalloc_header));
		newer->addr = header->addr;
		newer->size = size;
		newer->flag = USING;
		newer->node = avl_insert(&ecalloc->used_tree, newer);
		newer->next = header;

		newer->prev = header->prev;
		if (newer->prev) {
			newer->prev->next = newer;
		}

		header->prev = newer;
		header->addr += size;
		header->size -= size;
		header->node = avl_insert(&ecalloc->free_tree, header);
	} else {
		header->flag = USING;
		header->node = avl_insert(&ecalloc->used_tree, header);
	}

	pthread_mutex_unlock(&ecalloc->mutex);
	return ret;
}
