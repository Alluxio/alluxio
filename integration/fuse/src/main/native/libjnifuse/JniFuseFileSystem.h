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

#ifndef _JNI_FUSE_FILE_SYSTEM_H
#define _JNI_FUSE_FILE_SYSTEM_H

#include <fuse.h>

#include "jni.h"

namespace jnifuse {

class GetattrOperation;
class OpenOperation;
class Operation;
class ReaddirOperation;
class ReadOperation;
class UnlinkOperation;
class FlushOperation;
class ReleaseOperation;

struct  ThreadData {
  JavaVM *attachedJVM;
  JNIEnv *attachedEnv;
};


class JniFuseFileSystem {
 private:
  JniFuseFileSystem();
  ~JniFuseFileSystem();

 public:
  static JniFuseFileSystem *getInstance();
  void init(JNIEnv *env, jobject obj);
  JNIEnv *getEnv();
  JavaVM *getJVM();
  jobject getFSObj();

 private:
  JavaVM *jvm;
  jobject fs;

 public:
  GetattrOperation *getattrOper;
  OpenOperation *openOper;
  ReadOperation *readOper;
  ReaddirOperation *readdirOper;
  UnlinkOperation *unlinkOper;
  FlushOperation *flushOper;
  ReleaseOperation *releaseOper;
};

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

 private:
  jclass fillerclazz;
  jmethodID fillerconstructor;
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

}  // namespace jnifuse

#endif
