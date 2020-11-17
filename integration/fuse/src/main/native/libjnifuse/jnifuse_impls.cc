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

#include "jnifuse_impls.h"

#include "debug.h"
#include "jnifuse_fs.h"

int getattr_wrapper(const char *path, struct stat *stbuf) {
  LOGD("getattr %s", path);

  int ret =
      jnifuse::JniFuseFileSystem::getInstance()->getattrOper->call(path, stbuf);

  LOGD("file %s: size=%ld, mod=%d", path, stbuf->st_size, stbuf->st_mode);

  return ret;
}

int open_wrapper(const char *path, struct fuse_file_info *fi) {
  LOGD("open %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->openOper->call(path, fi);

  return ret;
}

int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
                 struct fuse_file_info *fi) {
  LOGD("read: %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->readOper->call(
      path, buf, size, offset, fi);

  LOGD("nread=%d", ret);

  return ret;
}

int readdir_wrapper(const char *path, void *buf, fuse_fill_dir_t filler,
                    off_t offset, struct fuse_file_info *fi) {
  LOGD("readdir: %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->readdirOper->call(
      path, buf, filler, offset, fi);

  return ret;
}

int unlink_wrapper(const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->unlinkOper->call(path);
}

int flush_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->flushOper->call(path, fi);
}

int release_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->releaseOper->call(path, fi);
}

int create_wrapper(const char *path, mode_t mode, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->createOper->call(path, mode,
                                                                     fi);
}

int mkdir_wrapper(const char *path, mode_t mode) {
  return jnifuse::JniFuseFileSystem::getInstance()->mkdirOper->call(path, mode);
}

int rmdir_wrapper(const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->rmdirOper->call(path);
}

int write_wrapper(const char *path, const char *buf, size_t size, off_t off,
                  struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->writeOper->call(
      path, buf, size, off, fi);
}

int rename_wrapper(const char *oldPath, const char *newPath) {
  return jnifuse::JniFuseFileSystem::getInstance()->renameOper->call(oldPath, newPath);
}
