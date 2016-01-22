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

struct bop_queue_item {
    int ubegin; // first recovery unit number
    int uend;   // last recovery unit number
    int nwait;  // number of recovery unit to wait
    struct conn *conn; // operation connection
};

struct bop_queue {
    int exit;
    int entry;
    // [exit, entry)
    int cap;
    struct bop_queue_item *items;
};

extern struct bop_queue *bopq;

void
bop_queue_init(void);

struct bop_queue_item *
bop_queue_add(int ubegin, int uend, int nwait, struct conn *conn);

void
bop_queue_invoke(int ubegin, int uend);

void
bop_queue_clean(void);
