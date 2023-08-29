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

#ifndef JNI_FUSE_CLI_H
#define JNI_FUSE_CLI_H

#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <cerrno>
#include <unistd.h>
#include <cstring>

// Define a tmp file for ioctl.
#define IOC_TMP_FILE  ".alluxio.fuse.cli"
#define MAX_FUSE_PATH_LEN 128
#define IOC_DATA_MAX_LENGTH 4096

typedef struct ioctl_args {
  int data_size;
  char data[IOC_DATA_MAX_LENGTH];
}ioctl_args_t;

enum {
  FIOC_GET_METADATA_SIZE	= _IOWR('V', 0, IOC_DATA_MAX_LENGTH),
  FIOC_CLEAR_METADATA	= _IOWR('V', 1, sizeof(ioctl_args_t)),
};

#endif // JNI_FUSE_CLI_H
