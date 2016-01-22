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
#include <stdlib.h>
#include <stdio.h>
#include "avltree.h"

static void avl_update_height(struct avlnode *node)
{
	uint64_t lheight, rheight;
	lheight = node->left ? node->left->height : 0;
	rheight = node->right ? node->right->height : 0;
	node->height = lheight > rheight? lheight + 1 : rheight + 1;
}

static void lrotate(struct avlnode **nodep)
{
	struct avlnode *node = *nodep;
	struct avlnode *parent = node->parent;
	struct avlnode *right = node->right;

	*nodep = right;
	right->parent = parent;
	node->right = right->left;
	if (right->left) {
		right->left->parent = node;
	}
	right->left = node;
	node->parent = right;

	avl_update_height(node);
	avl_update_height(right);
	if (parent) {
		avl_update_height(parent);
	}
}

static void rrotate(struct avlnode **nodep)
{
	struct avlnode *node = *nodep;
	struct avlnode *parent = node->parent;
	struct avlnode *left = node->left;

	*nodep = left;
	left->parent = parent;
	node->left = left->right;
	if (left->right) {
		left->right->parent = node;
	}
	left->right = node;
	node->parent = left;

	avl_update_height(node);
	avl_update_height(left);
	if (parent) {
		avl_update_height(parent);
	}
}

static void avl_nodeinit(struct avlnode *node, void *item, struct avlnode *parent)
{
	node->item = item;
	node->left = NULL;
	node->right = NULL;
	node->parent = parent;
	node->next = NULL;
	node->prev = NULL;
	node->height = 1;
}

static int avl_balance(struct avlnode *node)
{
	uint64_t lheight, rheight;
	lheight = node->left ? node->left->height : 0;
	rheight = node->right ? node->right->height : 0;
	return lheight - rheight;
}

void avl_init(struct avltree *tree,
        int32_t (*item_compare)(void *, void *),
        void (*move_item)(void *, struct avlnode *, struct avlnode *))
{
	tree->root = NULL;
	tree->head = NULL;
	tree->tail = NULL;
    tree->item_compare = item_compare;
    tree->move_item = move_item;
}

static void do_rotation(struct avltree *tree, struct avlnode *parent)
{
	struct avlnode *node;
	/* do rotation */
	while (parent) {
		int balance;
		struct avlnode *left, *right;
		node = parent;
		parent = node->parent;
		left = node->left;
		right = node->right;

		balance = avl_balance(node);

		if (balance < -1) {
			int son_balance = avl_balance(right);
			/* RR */
			if (son_balance <= 0) {
				if (!parent) {
					lrotate(&tree->root);
				} else {
					lrotate(node == parent->left ? &parent->left : &parent->right);
				}
				continue;
			}
			/* RL */
			if (son_balance > 0) {
				rrotate(&node->right);
				if (!parent) {
					lrotate(&tree->root);
				} else {
					lrotate(node == parent->left ? &parent->left : &parent->right);
				}
				continue;
			}
			assert(0);
		} else if (balance > 1) {
			int son_balance = avl_balance(left);
			/* LL */
			if (son_balance >= 0) {
				if (!parent) {
					rrotate(&tree->root);
				} else {
					rrotate(node == parent->left ? &parent->left : &parent->right);
				}
				continue;
			}
			/* LR */
			if (son_balance < 0) {
				lrotate(&node->left);
				if (!parent) {
					rrotate(&tree->root);
				} else {
					rrotate(node == parent->left ? &parent->left : &parent->right);
				}
				continue;
			}
			assert(0);
		} else {
			avl_update_height(node);
		}
	}
}

struct avlnode* avl_insert(struct avltree *tree, void *item)
{
	struct avlnode *node, *parent;
	struct avlnode **nodep;
	/* do insertion */
	parent = NULL;
	nodep = &tree->root;
	while ((node = *nodep)) {
		int32_t i = tree->item_compare(item, node->item);
        if (i <= 0) {
			parent = node;
			nodep = &node->left;
		} else if (i > 0) {
			parent = node;
			nodep = &node->right;
		} else {
			return node;
		}
	}
	node = safe_malloc(sizeof(struct avlnode));
	avl_nodeinit(node, item, parent);
	*nodep = node;
	if (!parent) {
		tree->head = node;
		tree->tail = node;
	}else if (parent->left == node) {
		struct avlnode *prev = parent->prev;
		node->prev = prev;
		node->next = parent;
		parent->prev = node;
		if (prev) {
			prev->next = node;
		} else {
			tree->head = node;
		}
	} else {
		struct avlnode *next = parent->next;
		node->next = next;
		node->prev = parent;
		parent->next = node;
		if (next) {
			next->prev = node;
		} else {
			tree->tail = node;
		}
	}

	do_rotation(tree, parent);
	return node;
}

void avl_remove_node(struct avltree *tree, struct avlnode *node)
{
	struct avlnode *parent, *right, *left, *prev, *next, *replace;

	while (1) {
		// no left son
		if (!node->left) {
			// modify tree stucture
			parent = node->parent;
			right = node->right;
			if (!parent) {
				tree->root = right;
			} else if (parent->left == node) {
				parent->left = right;
			} else {
				parent->right= right;
			}
			if (right) {
				right->parent = parent;
			}
			// modify double linked structure
			prev = node->prev;
			next = node->next;
			if (next) {
				next->prev = prev;
			} else {
				tree->tail = prev;
			}
			if (prev) {
				prev->next = next;
			} else {
				tree->head = next;
			}
			break;
		} else
		// no right son
		if (!node->right) {
			// modify tree structure
			parent = node->parent;
			left = node->left;
			if (!parent) {
				tree->root = left;
			} else if (parent->left == node) {
				parent->left = left;
			} else {
				parent->right= left;
			}
			if (left) {
				left->parent = parent;
			}
			// modify double linked structure
			prev = node->prev;
			next = node->next;
			if (next) {
				next->prev = prev;
			} else {
				tree->tail = prev;
			}
			if (prev) {
				prev->next = next;
			} else {
				tree->head = next;
			}
			break;
		}

		replace = node->prev;
		node->item = replace->item;
		tree->move_item(replace->item, replace, node);
		node = replace;
	}
	do_rotation(tree, parent);
	free(node);
}

void *avl_remove(struct avltree *tree, void *item)
{
	struct avlnode *node;
	void *ret;
	node = avl_search(tree, item);
	if (!node) return NULL;
	ret = node->item;
	avl_remove_node(tree, node);
	return ret;
}

struct avlnode* avl_search(struct avltree *tree, void *item)
{
	struct avlnode *node;
	/* do insertion */
	node = tree->root;
	while (node) {
		int32_t i = tree->item_compare(item, node->item);
		if (i < 0) {
			node = node->left;
		} else if (i > 0) {
			node = node->right;
		} else {
			break;
		}
	}
	return node;
}

struct avlnode* avl_search_close(struct avltree *tree, void *item)
{
	struct avlnode *node;
	/* do insertion */
	node = tree->root;
	while (node) {
		int32_t i = tree->item_compare(item, node->item);
		if (i < 0) {
			if (node->left) {
				node = node->left;
			} else {
				break;
			}
		} else if (i > 0) {
			if (node->right) {
				node = node->right;
			} else {
				break;
			}
		} else {
			break;
		}
	}
	return node;
}


