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

#ifndef FUSE_NATIVE_LIBJNIFUSE_FS_H_
#define FUSE_NATIVE_LIBJNIFUSE_FS_H_

#include <jni.h>

#include "operation.h"

namespace jnifuse {

class ChmodOperation;
class ChownOperation;
class CreateOperation;
class FlushOperation;
class GetattrOperation;
class GetxattrOperation;
class ListxattrOperation;
class MkdirOperation;
class OpenOperation;
class Operation;
class ReadOperation;
class ReaddirOperation;
class ReleaseOperation;
class RemovexattrOperation;
class RenameOperation;
class RmdirOperation;
class SetxattrOperation;
class TruncateOperation;
class UnlinkOperation;
class UtimensOperation;
class WriteOperation;
class SymlinkOperation;

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
  ChmodOperation *chmodOper;
  ChownOperation *chownOper;
  CreateOperation *createOper;
  FlushOperation *flushOper;
  GetattrOperation *getattrOper;
  GetxattrOperation *getxattrOper;
  ListxattrOperation *listxattrOper;
  MkdirOperation *mkdirOper;
  OpenOperation *openOper;
  ReadOperation *readOper;
  ReaddirOperation *readdirOper;
  ReleaseOperation *releaseOper;
  RemovexattrOperation *removexattrOper;
  RenameOperation *renameOper;
  RmdirOperation *rmdirOper;
  SetxattrOperation *setxattrOper;
  SymlinkOperation *symlinkOper;
  TruncateOperation *truncateOper;
  UnlinkOperation *unlinkOper;
  UtimensOperation *utimensOper;
  WriteOperation *writeOper;
};

}  // namespace jnifuse

#endif  // FUSE_NATIVE_LIBJNIFUSE_FS_H_
