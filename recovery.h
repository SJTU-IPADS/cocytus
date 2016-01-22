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
#ifndef RECOVERY_H
#define RECOVERY_H

#include <stdint.h>
#include "ecmem.h"
#include <jerasure.h>
#include <reed_sol.h>
#include <galois.h>
#include "const.h"

#define GET_FLAG(unit,offset) (((unit)->flags) & (1<<(offset)))
#define SET_FLAG(unit,offset) do { \
        ((unit)->flags) |= (1<<(offset)); \
} while (0)

// is this unit need updates (during recovery)
// updates should also be applied to this unit
#define GET_UPDATE_FLAG(unit) GET_FLAG(unit,30)
#define SET_UPDATE_FLAG(unit) SET_FLAG(unit,30)

// is this unit already recovered
//  ready for get/set operations
#define GET_RECOVERED_FLAG(unit) GET_FLAG(unit,31)
#define SET_RECOVERED_FLAG(unit) SET_FLAG(unit,31)

#define GET_PEER_FLAG(unit,peer) GET_FLAG(unit,peer)
#define SET_PEER_FLAG(unit,peer) SET_FLAG(unit,peer)


struct recovery_unit {
    uint32_t flags;
    char *data;
};


struct recovery_queue_item {
    struct conn *conn;
    int req_lid;
    int leader_lid;
    int unit_begin;
    int unit_end;
    uint32_t mask;
    uint32_t flags;
    int in_use; // 1 for in_use; -1 for in_abort; 0 for free
    // lid indexed buffer
    char **data_from_parity;
};
struct recovery_queue {
    int cap;
    struct recovery_queue_item *items;
    int entry;
    int exit;
    // [exit, entry)
};

struct recovery {
    struct recovery_unit *units; // tmp units for current recovery
                                // the final result of the recovery will be stored in sub_ecmem
    struct recovery_queue queue;
};

void
recovery_req_clean(struct recovery *r);

struct recovery_queue_item *
recovery_req_add(
        struct recovery *r, int req_lid, struct conn *conn, int leader,
        int unit_begin, int unit_end, uint32_t mask);

struct recovery_queue_item *
recovery_req_find(
        struct recovery *r, int leader, int unit_begin, int unit_end, uint32_t mask);

int
recovery_check_completeness(struct recovery *r, int rid, uint32_t check_mask);

void
recovery_init(struct recovery *r);

struct recovery_unit *
recovery_get_unit(struct recovery *r, uint64_t addr);

void
recovery_recover_units(
        struct recovery *r, struct ecmem *ecm, int peerid,
        int ubegin, int uend, char *data);

void
recovery_update_unit(struct recovery *r, int peerid, uint64_t addr, char *data);

int
recovery_calc_unit_id(struct recovery *r, uint64_t addr);

uint64_t
recovery_calc_start_addr(struct recovery *r, int unit_id);

int
recovery_calc_total_length(struct recovery *r, int unit_begin, int unit_end);

int
recovery_try_update_unit(
        struct recovery *r, int peerid, uint64_t addr,
        char *data, uint32_t size);

void
recovery_recover_units_gather(
        struct recovery *r, struct ecmem *ecm,
        int peerid, int rid, char *data, int len);

void
recovery_req_remove(struct recovery *r,
        struct recovery_queue_item *rqit);

#endif
