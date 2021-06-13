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

#include "jnifuse_fs.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <jni.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"

namespace jnifuse {

struct ThreadData {
  JavaVM *attachedJVM;
  JNIEnv *attachedEnv;
};

static pthread_key_t jffs_threadKey;

static void thread_data_free(void *ptr) {
  ThreadData *td = (ThreadData *)ptr;
  if (td->attachedJVM != nullptr) {
    td->attachedJVM->DetachCurrentThread();
  }
  delete td;
}

JniFuseFileSystem *JniFuseFileSystem::instance = nullptr;

JniFuseFileSystem::JniFuseFileSystem(JNIEnv *env, jobject obj) {
  env->GetJavaVM(&this->jvm);
  this->fs = env->NewGlobalRef(obj);

  this->chmodOper = new ChmodOperation(this);
  this->chownOper = new ChownOperation(this);
  this->createOper = new CreateOperation(this);
  this->flushOper = new FlushOperation(this);
  this->getattrOper = new GetattrOperation(this);
  this->getxattrOper = new GetxattrOperation(this);
  this->listxattrOper = new ListxattrOperation(this);
  this->mkdirOper = new MkdirOperation(this);
  this->openOper = new OpenOperation(this);
  this->readOper = new ReadOperation(this);
  this->readdirOper = new ReaddirOperation(this);
  this->releaseOper = new ReleaseOperation(this);
  this->removexattrOper = new RemovexattrOperation(this);
  this->renameOper = new RenameOperation(this);
  this->rmdirOper = new RmdirOperation(this);
  this->setxattrOper = new SetxattrOperation(this);
  this->symlinkOper = new SymlinkOperation(this);
  this->truncateOper = new TruncateOperation(this);
  this->unlinkOper = new UnlinkOperation(this);
  this->utimensOper = new UtimensOperation(this);
  this->writeOper = new WriteOperation(this);
}

JniFuseFileSystem::~JniFuseFileSystem() {
  delete this->chmodOper;
  delete this->chownOper;
  delete this->createOper;
  delete this->flushOper;
  delete this->getattrOper;
  delete this->getxattrOper;
  delete this->listxattrOper;
  delete this->mkdirOper;
  delete this->openOper;
  delete this->readOper;
  delete this->readdirOper;
  delete this->releaseOper;
  delete this->removexattrOper;
  delete this->renameOper;
  delete this->rmdirOper;
  delete this->setxattrOper;
  delete this->truncateOper;
  delete this->unlinkOper;
  delete this->utimensOper;
  delete this->writeOper;
}

void JniFuseFileSystem::init(JNIEnv *env, jobject obj) {
  if (instance != nullptr) {
    LOGE("you cant initialize more than once");
  }
  pthread_key_create(&jffs_threadKey, thread_data_free);
  instance = new JniFuseFileSystem(env, obj);
}

JniFuseFileSystem *JniFuseFileSystem::getInstance() {
  if (instance == nullptr) {
    LOGE("you must initialize before using JniFuseFileSystem");
    exit(-1);
  }
  return instance;
}

JNIEnv *JniFuseFileSystem::getEnv() {
  ThreadData *td = (ThreadData *)pthread_getspecific(jffs_threadKey);
  if (td == nullptr) {
    td = new ThreadData();
    td->attachedJVM = this->jvm;
    this->jvm->AttachCurrentThreadAsDaemon((void **)&td->attachedEnv, nullptr);
    pthread_setspecific(jffs_threadKey, td);
    return td->attachedEnv;
  }
  return td->attachedEnv;
}

JavaVM *JniFuseFileSystem::getJVM() { return this->jvm; }

jobject JniFuseFileSystem::getFSObj() { return this->fs; }

}  // namespace jnifuse
