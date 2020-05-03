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

#include "JniFuseFileSystem.h"

#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <jni.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"

using namespace jnifuse;

static pthread_key_t jffs_threadKey;

static void thread_data_free(void *ptr) {
  ThreadData* td = (ThreadData *)ptr;
  if (td->attachedJVM != nullptr) {
    td->attachedJVM->DetachCurrentThread();
  }
  delete td;
}

JniFuseFileSystem::JniFuseFileSystem() {
  this->fs = nullptr;
  this->jvm = nullptr;

  this->getattrOper = nullptr;
  this->openOper = nullptr;
  this->readOper = nullptr;
  this->readdirOper = nullptr;
  this->unlinkOper = nullptr;
  this->flushOper = nullptr;
  this->releaseOper = nullptr;
}

JniFuseFileSystem::~JniFuseFileSystem() {
  delete this->getattrOper;
  delete this->openOper;
  delete this->readOper;
  delete this->readOper;
  delete this->unlinkOper;
  delete this->flushOper;
  delete this->releaseOper;
}

void JniFuseFileSystem::init(JNIEnv *env, jobject obj) {
  env->GetJavaVM(&this->jvm);
  this->fs = env->NewGlobalRef(obj);

  this->getattrOper = new GetattrOperation(this);
  this->openOper = new OpenOperation(this);
  this->readOper = new ReadOperation(this);
  this->readdirOper = new ReaddirOperation(this);
  this->unlinkOper = new UnlinkOperation(this);
  this->flushOper = new FlushOperation(this);
  this->releaseOper = new ReleaseOperation(this);

  pthread_key_create(&jffs_threadKey, thread_data_free);
}

JniFuseFileSystem *JniFuseFileSystem::getInstance() {
  static JniFuseFileSystem *instance = nullptr;
  if (instance == nullptr) {
    instance = new JniFuseFileSystem();
  }
  return instance;
}

JNIEnv *JniFuseFileSystem::getEnv() {
  ThreadData *td = (ThreadData *)pthread_getspecific(jffs_threadKey);
  if (td == nullptr) {
    td = new ThreadData();
    td->attachedJVM = this->jvm;
    this->jvm->AttachCurrentThreadAsDaemon((void **)&td->attachedEnv, NULL);
    pthread_setspecific(jffs_threadKey, td);
    return td->attachedEnv;
  }
  td = (ThreadData *)pthread_getspecific(jffs_threadKey);
  return td->attachedEnv;
}

JavaVM *JniFuseFileSystem::getJVM() {
  return this->jvm;
}

jobject JniFuseFileSystem::getFSObj() { return this->fs; }

Operation::Operation() {
  this->fs = nullptr;
  this->obj = nullptr;
  this->clazz = nullptr;
  this->signature = nullptr;
  this->methodID = nullptr;
}

Operation::~Operation() {}

GetattrOperation::GetattrOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "getattrCallback", signature);
}

int GetattrOperation::call(const char *path, struct stat *stbuf) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject buffer = env->NewDirectByteBuffer((void *)stbuf, sizeof(struct stat));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buffer);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(buffer);

  return ret;
}

OpenOperation::OpenOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "openCallback", signature);
}

int OpenOperation::call(const char *path, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(fibuf);

  return ret;
}

ReadOperation::ReadOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature =
      "(Ljava/lang/String;Ljava/nio/ByteBuffer;JJLjava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "readCallback", signature);
}

int ReadOperation::call(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject buffer = env->NewDirectByteBuffer((void *)buf, size);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buffer, size,
                               offset, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(buffer);
  env->DeleteLocalRef(fibuf);

  return ret;
}

ReaddirOperation::ReaddirOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature =
      "(Ljava/lang/String;JLalluxio/jnifuse/FuseFillDir;JLjava/nio/"
      "ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "readdirCallback", signature);

  this->fillerclazz = env->FindClass("alluxio/jnifuse/FuseFillDir");
  this->fillerconstructor = env->GetMethodID(fillerclazz, "<init>", "(J)V");
}

int ReaddirOperation::call(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jobject fillerobj = env->NewObject(this->fillerclazz, this->fillerconstructor,
                                     (void *)filler);
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buf,
                               fillerobj, offset, fibuf);

  env->DeleteLocalRef(fillerobj);
  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(fibuf);

  return ret;
}

UnlinkOperation::UnlinkOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;)I";
  this->methodID = env->GetMethodID(this->clazz, "unlinkCallback", signature);
}

int UnlinkOperation::call(const char *path) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath);

  env->DeleteLocalRef(jspath);

  return ret;
}

FlushOperation::FlushOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "flushCallback", signature);
}

int FlushOperation::call(const char *path, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(fibuf);

  return ret;
}

ReleaseOperation::ReleaseOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "releaseCallback", signature);
}

int ReleaseOperation::call(const char *path, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(fibuf);

  return ret;
}
