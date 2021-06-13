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

#ifndef FUSE_NATIVE_LIBJNIFUSE_OPERATION_H_
#define FUSE_NATIVE_LIBJNIFUSE_OPERATION_H_

#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION 26
#endif
#include <fuse.h>
#include <jni.h>

#include "jnifuse_fs.h"

namespace jnifuse {

class JniFuseFileSystem;

class Operation {
 protected:
  JniFuseFileSystem *fs;
  jclass clazz;
  jobject obj;
  jmethodID methodID;
  const char *signature;

 public:
  Operation();
  virtual ~Operation();
};

class GetattrOperation : public Operation {
 public:
  GetattrOperation(JniFuseFileSystem *fs);
  int call(const char *path, struct stat *stbuf);
};

class OpenOperation : public Operation {
 public:
  OpenOperation(JniFuseFileSystem *fs);
  int call(const char *path, struct fuse_file_info *fi);
};

class ReadOperation : public Operation {
 public:
  ReadOperation(JniFuseFileSystem *fs);
  int call(const char *path, char *buf, size_t size, off_t offset,
           struct fuse_file_info *fi);
};

class ReaddirOperation : public Operation {
 public:
  ReaddirOperation(JniFuseFileSystem *fs);
  int call(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
           struct fuse_file_info *fi);
};

class UnlinkOperation : public Operation {
 public:
  UnlinkOperation(JniFuseFileSystem *fs);
  int call(const char *path);
};

class FlushOperation : public Operation {
 public:
  FlushOperation(JniFuseFileSystem *fs);
  int call(const char *path, struct fuse_file_info *fi);
};

class ReleaseOperation : public Operation {
 public:
  ReleaseOperation(JniFuseFileSystem *fs);
  int call(const char *path, struct fuse_file_info *fi);
};

class ChmodOperation : public Operation {
 public:
  ChmodOperation(JniFuseFileSystem *fs);
  int call(const char *path, mode_t mode);
};

class ChownOperation : public Operation {
 public:
  ChownOperation(JniFuseFileSystem *fs);
  int call(const char *path, uid_t uid, gid_t gid);
};

class CreateOperation : public Operation {
 public:
  CreateOperation(JniFuseFileSystem *fs);
  int call(const char *path, mode_t mode, struct fuse_file_info *fi);
};

class MkdirOperation : public Operation {
 public:
  MkdirOperation(JniFuseFileSystem *fs);
  int call(const char *path, mode_t mode);
};

class RenameOperation : public Operation {
 public:
  RenameOperation(JniFuseFileSystem *fs);
  int call(const char *oldPath, const char *newPath);
};

class RmdirOperation : public Operation {
 public:
  RmdirOperation(JniFuseFileSystem *fs);
  int call(const char *path);
};

class StatfsOperation : public Operation {
 public:
  StatfsOperation(JniFuseFileSystem *fs);
  int call(const char *path, struct statvfs *st);
};

class TruncateOperation : public Operation {
 public:
  TruncateOperation(JniFuseFileSystem *fs);
  int call(const char *path, off_t off);
};

class WriteOperation : public Operation {
 public:
  WriteOperation(JniFuseFileSystem *fs);
  int call(const char *path, const char *buf, size_t size, off_t off,
           struct fuse_file_info *fi);
};

class GetxattrOperation : public Operation {
 public:
  GetxattrOperation(JniFuseFileSystem *fs);
  int call(const char *path, const char *name, char *value, size_t size);
};

class SetxattrOperation : public Operation {
 public:
  SetxattrOperation(JniFuseFileSystem *fs);
  int call(const char *path, const char *name, const char *value,
           size_t size, int flags);
};

class SymlinkOperation : public Operation {
 public:
  SymlinkOperation(JniFuseFileSystem *fs);
  int call(const char *linkname, const char *path);
};

class ListxattrOperation : public Operation {
 public:
  ListxattrOperation(JniFuseFileSystem *fs);
  int call(const char *path, char *list, size_t size);
};

class RemovexattrOperation : public Operation {
 public:
  RemovexattrOperation(JniFuseFileSystem *fs);
  int call(const char *path, const char *list);
};

class UtimensOperation : public Operation {
 public:
  UtimensOperation(JniFuseFileSystem *fs);
  int call(const char *path, const struct timespec ts[2]);
};

}  // namespace jnifuse

#endif  // FUSE_NATIVE_LIBJNIFUSE_OPERATION_H_
