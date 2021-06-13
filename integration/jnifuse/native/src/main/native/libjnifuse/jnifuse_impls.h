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

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#include <fuse.h>

int chmod_wrapper(const char *path, mode_t mode);
int chown_wrapper(const char *path, uid_t uid, gid_t gid);
int create_wrapper(const char *path, mode_t mode, struct fuse_file_info *fi);
int flush_wrapper(const char *path, struct fuse_file_info *fi);
int getattr_wrapper(const char *path, struct stat *stbuf);
#ifdef __APPLE__
int getxattr_wrapper(const char *path, const char *name, char *value, size_t size, uint32_t position);
int setxattr_wrapper(const char *path, const char *name,
                     const char *value, size_t size, int flags, uint32_t position);
#else
int getxattr_wrapper(const char *path, const char *name, char *value, size_t size);
int setxattr_wrapper(const char *path, const char *name,
                     const char *value, size_t size, int flags);
#endif
int listxattr_wrapper(const char *path, char *list, size_t size);
int mkdir_wrapper(const char *path, mode_t mode);
int open_wrapper(const char *path, struct fuse_file_info *fi);
int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
                 struct fuse_file_info *fi);
int readdir_wrapper(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi);
int release_wrapper(const char *path, struct fuse_file_info *fi);
int removexattr_wrapper(const char *path, const char *list);
int rename_wrapper(const char *oldPath, const char *newPath);
int rmdir_wrapper(const char *path);
int symlink_wrapper(const char *linkname, const char *path);
int truncate_wrapper(const char *path, off_t size);
int unlink_wrapper(const char *path);
int utimens_wrapper(const char *path, const struct timespec ts[2]);
int write_wrapper(const char *path, const char *buf, size_t size, off_t off,
                  struct fuse_file_info *fi);

#endif  // FUSE_NATIVE_LIBJNIFUSE_IMPLS_H_
