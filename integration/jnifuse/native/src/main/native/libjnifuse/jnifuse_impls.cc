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

int chmod_wrapper(const char *path, mode_t mode) {
  return jnifuse::JniFuseFileSystem::getInstance()->chmodOper->call(path, mode);
}

int chown_wrapper(const char *path, uid_t uid, gid_t gid) {
  return jnifuse::JniFuseFileSystem::getInstance()->chownOper->call(path, uid, gid);
}

int create_wrapper(const char *path, mode_t mode, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->createOper->call(path, mode,
                                                                     fi);
}

int flush_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->flushOper->call(path, fi);
}

int getattr_wrapper(const char *path, struct stat *stbuf) {
  LOGD("getattr %s", path);

  int ret =
      jnifuse::JniFuseFileSystem::getInstance()->getattrOper->call(path, stbuf);

  LOGD("file %s: size=%ld, mod=%d", path, stbuf->st_size, stbuf->st_mode);

  return ret;
}

#ifdef __APPLE__
int getxattr_wrapper(const char *path, const char *name, char *value, size_t size, uint32_t position) {
  return jnifuse::JniFuseFileSystem::getInstance()->getxattrOper->call(path, name, value, size);
}
#else
int getxattr_wrapper(const char *path, const char *name, char *value, size_t size) {
  return jnifuse::JniFuseFileSystem::getInstance()->getxattrOper->call(path, name, value, size);
}
#endif

int listxattr_wrapper(const char *path, char *list, size_t size) {
  return jnifuse::JniFuseFileSystem::getInstance()->listxattrOper->call(path, list, size);
}

int mkdir_wrapper(const char *path, mode_t mode) {
  return jnifuse::JniFuseFileSystem::getInstance()->mkdirOper->call(path, mode);
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

int release_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->releaseOper->call(path, fi);
}

int removexattr_wrapper(const char *path, const char *list) {
  return jnifuse::JniFuseFileSystem::getInstance()->removexattrOper->call(path, list);
}

int rename_wrapper(const char *oldPath, const char *newPath) {
  return jnifuse::JniFuseFileSystem::getInstance()->renameOper->call(oldPath, newPath);
}

int rmdir_wrapper(const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->rmdirOper->call(path);
}

#ifdef __APPLE__
int setxattr_wrapper(const char *path, const char *name,
                     const char *value, size_t size, int flags, uint32_t position) {
  return jnifuse::JniFuseFileSystem::getInstance()->setxattrOper->call(path, name, value, size, flags);
}
#else
int setxattr_wrapper(const char *path, const char *name,
                     const char *value, size_t size, int flags) {
  return jnifuse::JniFuseFileSystem::getInstance()->setxattrOper->call(path, name, value, size, flags);
}
#endif

int symlink_wrapper(const char *linkname, const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->symlinkOper->call(linkname, path);
}

int truncate_wrapper(const char *path, off_t size) {
  return jnifuse::JniFuseFileSystem::getInstance()->truncateOper->call(path, size);
}

int unlink_wrapper(const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->unlinkOper->call(path);
}

int utimens_wrapper(const char *path, const struct timespec ts[2]) {
  return jnifuse::JniFuseFileSystem::getInstance()->utimensOper->call(path, ts);
}

int write_wrapper(const char *path, const char *buf, size_t size, off_t off,
                  struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->writeOper->call(
      path, buf, size, off, fi);
}
