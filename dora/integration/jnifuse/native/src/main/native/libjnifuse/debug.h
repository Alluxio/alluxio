/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#ifndef FUSE_NATIVE_LIBJNIFUSE_DEBUG_H_
#define FUSE_NATIVE_LIBJNIFUSE_DEBUG_H_

#include <stdio.h>

#ifdef DEBUG
#define LOGD(format, ...) \
do { \
    fprintf(stdout, "DEBUG %s:%d " format "\n", \
        __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)
#else
#define LOGD(format, ...)
#endif

#define LOGI(format, ...) \
do { \
    fprintf(stdout, "INFO %s:%d " format "\n", \
        __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)

#define LOGE(format, ...) \
do { \
    fprintf(stderr, "ERROR %s:%d " format "\n", \
        __FILE__, __LINE__, ##__VA_ARGS__); \
} while (0)

#endif  // FUSE_NATIVE_LIBJNIFUSE_DEBUG_H_
