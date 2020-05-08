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
#include "operation.h"

namespace jnifuse {

class GetattrOperation;
class OpenOperation;
class Operation;
class ReaddirOperation;
class ReadOperation;
class UnlinkOperation;
class FlushOperation;
class ReleaseOperation;
class CreateOperation;
class MkdirOperation;
class RmdirOperation;
class WriteOperation;

struct ThreadData {
  JavaVM *attachedJVM;
  JNIEnv *attachedEnv;
};

class JniFuseFileSystem {
 private:
  JniFuseFileSystem(JNIEnv *env, jobject obj);
  ~JniFuseFileSystem();

 public:
  static JniFuseFileSystem *getInstance();
  static void init(JNIEnv *env, jobject obj);
  JNIEnv *getEnv();
  JavaVM *getJVM();
  jobject getFSObj();

 private:
  static JniFuseFileSystem *instance;
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
  CreateOperation *createOper;
  MkdirOperation *mkdirOper;
  RmdirOperation *rmdirOper;
  WriteOperation *writeOper;
};

}  // namespace jnifuse

#endif
