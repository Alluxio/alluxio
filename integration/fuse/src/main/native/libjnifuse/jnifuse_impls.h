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

#ifndef FUSE_NATIVE_LIBJNIFUSE_IMPLS_H_
#define FUSE_NATIVE_LIBJNIFUSE_IMPLS_H_

#include <fuse.h>

int getattr_wrapper(const char *path, struct stat *stbuf);
int open_wrapper(const char *path, struct fuse_file_info *fi);
int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
                 struct fuse_file_info *fi);
int readdir_wrapper(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi);
int unlink_wrapper(const char *path);
int flush_wrapper(const char *path, struct fuse_file_info *fi);
int release_wrapper(const char *path, struct fuse_file_info *fi);
int create_wrapper(const char *path, mode_t mode, struct fuse_file_info *fi);
int mkdir_wrapper(const char *path, mode_t mode);
int rmdir_wrapper(const char *path);
int write_wrapper(const char *path, const char *buf, size_t size, off_t off,
                  struct fuse_file_info *fi);

#endif  // FUSE_NATIVE_LIBJNIFUSE_IMPLS_H_
