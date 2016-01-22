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
#include <time.h>
#include <sys/time.h>

#include <galois.h>

int main()
{
    int size = 512 << 20;
    char *src = malloc(size);
    int i;
    for (i=0; i<size; ++i) {
        *(src+i) = i % 20;
    }
    char *diff = malloc(size);
    for (i=0; i<size; ++i) {
        *(diff+i) = i % 20;
    }
    struct timespec s, e;
    clock_gettime(CLOCK_MONOTONIC, &s);
    galois_w08_region_multiply(diff,
            2, size,
            src, 1);
    clock_gettime(CLOCK_MONOTONIC, &e);

    long long during = (long long)(e.tv_sec - s.tv_sec);
    printf("%lld s ", during);
    during = (long long)(e.tv_nsec - s.tv_nsec);
    printf("%lld ns\n", during);
    return 0;
}
