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
#ifndef AVLTREE_H
#define AVLTREE_H

#include "def.h"

struct avlnode {
	void* item;
	struct avlnode* left;
	struct avlnode* right;
	struct avlnode* parent;
	struct avlnode* next;
	struct avlnode* prev;
	uint64_t height;
};

/*
** Hence, because item_a and item_b may be different type.
** We define that item_a is input and item_b is stored in tree.
*/
struct avltree {
	struct avlnode *root;
	struct avlnode *head;
	struct avlnode *tail;
	int32_t (*item_compare)(void *item_a, void *item_b);
	void (*move_item)(void *, struct avlnode *, struct avlnode *);
};

void avl_init(struct avltree *tree,
        int32_t (*item_compare)(void *item_a, void *item_b),
        void (*move_item)(void *, struct avlnode *, struct avlnode *));

struct avlnode* avl_insert(struct avltree *tree, void *item);

void *avl_remove(struct avltree *tree, void *item);

struct avlnode* avl_search(struct avltree *tree, void *item);

void avl_remove_node(struct avltree *tree, struct avlnode *node);

struct avlnode* avl_search_close(struct avltree *tree, void *item);

#endif
