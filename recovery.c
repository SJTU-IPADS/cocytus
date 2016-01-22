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
#include "recovery.h"

#include <stdlib.h>
#include <string.h>
#include "memcached.h"
#include "ecmem.h"

extern struct ecmem *sub_ecmem;
extern char *sub_flags;
extern struct peer *peers;

void
recovery_init(struct recovery *r)
{
    // init param
    int nunit = MEMSIZE / UNITSIZE;

    // alloc for units
    r->units = safe_malloc(nunit * sizeof(struct recovery_unit));

    // init all units
    memset(r->units, 0, sizeof(struct recovery_unit) * nunit);

    // init recovery queue
    struct recovery_queue *rq = &r->queue;
    rq->cap = 4096;
    rq->items = malloc(sizeof(struct recovery_queue_item) * rq->cap);
    rq->entry = 0;
    rq->exit = 0;
}

inline struct recovery_unit *
recovery_get_unit(struct recovery *r, uint64_t addr)
{
    assert(addr % UNITSIZE == 0);
    return &r->units[addr / UNITSIZE];
}

void
recovery_recover_units(
        struct recovery *r, struct ecmem *ecm, int peerid,
        int ubegin, int uend, char *data)
{
    // the recover units are already been cleaned after the previous recovery
    int i;
    for (i=ubegin; i<=uend; ++i) {
        struct recovery_unit *unit = &r->units[i];
        debug("uflags for unit %d: %x", i, unit->flags);

        // not recovered
        assert(! GET_RECOVERED_FLAG(unit));
        // this peer's data have not been applied yet
        assert(! GET_PEER_FLAG(unit,peerid));

        if (GET_UPDATE_FLAG(unit) == 0) { // first touch
            // alloc and copy parity data
            assert(unit->data == NULL);
            unit->data = malloc(UNITSIZE);
            assert(unit->data);
            memcpy(unit->data, ecmem_get(ecm, i*UNITSIZE),
                    UNITSIZE);
            // set update_flags
            SET_UPDATE_FLAG(unit);
            SET_PEER_FLAG(unit, settings.lid);
        }

        // set peer flag
        SET_PEER_FLAG(unit, peerid);
        // apply patch to the unit
        galois_w08_region_multiply(data,
                MATRIX(settings.lid, peerid), UNITSIZE,
                unit->data, 1);
        data += UNITSIZE;
    }
}

int
recovery_try_update_unit(
        struct recovery *r, int peerid, uint64_t addr,
        char *data, uint32_t size)
{
    int ret = 0;

    while (size > 0) {
        uint64_t offset = addr % UNITSIZE;
        uint64_t base = addr - offset;
        int len = UNITSIZE - offset;
        if (size < len) len = size;
        size -= len;
        // update touch_flags
        peers[peerid].touch_flags[base / UNITSIZE] = 1;
        if (sub_flags==NULL || sub_flags[base / UNITSIZE] != 2) ret++;
        struct recovery_unit *unit = recovery_get_unit(r, base);
        // already recovered
        if (GET_RECOVERED_FLAG(unit)) goto done;
        // don't need update
        if (!GET_UPDATE_FLAG(unit)) goto done;
        // this peer is already done
        if (GET_PEER_FLAG(unit,peerid)) goto done;

        // TODO: apply the data
        galois_w08_region_multiply(data,
                MATRIX(settings.lid, peerid), len,
                unit->data + offset, 1);
done:
        addr += len;
        data += len;
    }
    return ret;
}

inline int
recovery_calc_unit_id(struct recovery *r, uint64_t addr)
{
    return addr / UNITSIZE;
}

inline uint64_t
recovery_calc_start_addr(struct recovery *r, int unit_id)
{
    return unit_id * UNITSIZE;
}

inline int
recovery_calc_total_length(struct recovery *r, int unit_begin, int unit_end)
{
    return UNITSIZE * (unit_end - unit_begin + 1);
}

struct recovery_queue_item *
recovery_req_find(
        struct recovery *r, int leader, int unit_begin, int unit_end, uint32_t mask)
{
    struct recovery_queue *rq = &r->queue;
    int i;
    for (i=rq->exit; i<rq->entry; ++i) {
        struct recovery_queue_item *item = &rq->items[i % rq->cap];
        debug("find %d [%d,%d] %"PRIu32"; got %d [%d,%d] %"PRIu32"\n",
                leader, unit_begin, unit_end, mask,
                item->leader_lid, item->unit_begin, item->unit_end, item->mask);
        // TODO: go on compare
        if (item->leader_lid == leader
                && item->unit_begin == unit_begin
                && item->unit_end == unit_end) {
            //
            if (item->mask == mask) return item;
            // remove old ones
            //recovery_req_remove(r, item);
        }
    }
    return NULL;
}

void
recovery_req_clean(struct recovery *r)
{
    struct recovery_queue *rq = &r->queue;
    int *entry = &rq->entry;
    int *exit = &rq->exit;
    // [exit, entry)
    while (*exit < *entry) {
        if (0 == rq->items[*exit % rq->cap].in_use) {
            *exit += 1;
        } else break;
    }
}

void
recovery_req_remove(
        struct recovery *r, struct recovery_queue_item *rqit)
{
    struct recovery_queue_item *item = rqit;
    //free(item->data_from_parity);
    //item->data_from_parity = NULL;
    if (item->in_use != -1) {
        // -1 is already cleaned
        int i;
        for (i=item->unit_begin; i<=item->unit_end; ++i) {
            struct recovery_unit *unit = &r->units[i];
            // clean the recovery unit
            if (unit->data) free(unit->data);
            unit->data = NULL;
            unit->flags = 0;
            // no need to change the sub_flags, all requests will be restarted
            //if (sub_flags[i] == 1) sub_flags[i] = 0;
        }
    }
    item->in_use = 0;
    recovery_req_clean(r);
}

struct recovery_queue_item *
recovery_req_add(
        struct recovery *r, int req_lid, conn *conn, int leader,
        int unit_begin, int unit_end, uint32_t mask)
{
    struct recovery_queue *rq = &r->queue;
    if (rq->exit > rq->cap) {
        rq->exit -= rq->cap;
        rq->entry -= rq->cap;
    }
    recovery_req_clean(r);
    if (rq->entry - rq->exit >= rq->cap) {
        return NULL;
    }
    struct recovery_queue_item *item = &rq->items[rq->entry % rq->cap];
    item->conn = conn;
    item->req_lid = req_lid;
    item->leader_lid = leader;
    item->unit_begin = unit_begin;
    item->unit_end = unit_end;
    item->mask = mask;
    item->flags = 0;
    item->in_use = 1;
    item->data_from_parity =
        calloc(sizeof(char *), (settings.nparity + settings.nshard));
    int i;
    for (i=unit_begin; i<=unit_end; ++i) {
        // don't touch those who are already been recovered
        if (sub_flags[i] == 0) sub_flags[i] = 1;
    }
    rq->entry++;
    return item;
}
