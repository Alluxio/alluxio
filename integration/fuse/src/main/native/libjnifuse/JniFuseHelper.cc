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

#define FUSE_USE_VERSION 26

#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "JniFuseFileSystem.h"
#include "debug.h"

#ifdef __cplusplus
extern "C" {
#endif

static int getattr_wrapper(const char *path, struct stat *stbuf) {
  LOGD("getattr %s", path);

  int ret =
      jnifuse::JniFuseFileSystem::getInstance()->getattrOper->call(path, stbuf);

  LOGD("file %s: size=%ld, mod=%d", path, stbuf->st_size, stbuf->st_mode);

  return ret;
}

static int open_wrapper(const char *path, struct fuse_file_info *fi) {
  LOGD("open %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->openOper->call(path, fi);

  return ret;
}

static int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
                        struct fuse_file_info *fi) {
  LOGD("read: %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->readOper->call(
      path, buf, size, offset, fi);

  LOGD("nread=%d", ret);

  return ret;
}

static int readdir_wrapper(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi) {
  LOGD("readdir: %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->readdirOper->call(
      path, buf, filler, offset, fi);

  return ret;
}

static int unlink_wrapper(const char *path) {
  return jnifuse::JniFuseFileSystem::getInstance()->unlinkOper->call(path);
}

static int flush_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->flushOper->call(path, fi);
}

static int release_wrapper(const char *path, struct fuse_file_info *fi) {
  return jnifuse::JniFuseFileSystem::getInstance()->releaseOper->call(path, fi);
}

// TODO: Add more operations
static struct fuse_operations jnifuse_oper;

JNIEXPORT jint JNICALL Java_alluxio_jnifuse_LibFuse_fuse_1main_1real(
    JNIEnv *env, jobject libfuseobj, jobject obj, jint jargc,
    jobjectArray jargv) {
  LOGD("enter fuse_main_real");

  jnifuse::JniFuseFileSystem *fs = jnifuse::JniFuseFileSystem::getInstance();
  fs->init(env, obj);

  int argc = jargc;
  LOGD("argc=%d", argc);

  char **argv = (char **)malloc(sizeof(char *) * argc);
  int i;
  for (i = 0; i < argc; i++) {
    jstring str = (jstring)env->GetObjectArrayElement(jargv, i);
    argv[i] = (char *)env->GetStringUTFChars(str, 0);
    LOGD("argv[%d]=%s", i, argv[i]);
  }

  jnifuse_oper.getattr = getattr_wrapper;
  jnifuse_oper.open = open_wrapper;
  jnifuse_oper.read = read_wrapper;
  jnifuse_oper.readdir = readdir_wrapper;
  jnifuse_oper.unlink = unlink_wrapper;
  jnifuse_oper.flush = flush_wrapper;
  jnifuse_oper.release = release_wrapper;

  return fuse_main_real(argc, argv, &jnifuse_oper,
                        sizeof(struct fuse_operations), NULL);
}

jint JNICALL Java_alluxio_jnifuse_FuseFillDir_fill(JNIEnv *env, jobject obj,
                                                   jlong address, jlong bufaddr,
                                                   jstring name, jobject stbuf,
                                                   jlong off) {
  LOGD("enter fill");
  fuse_fill_dir_t filler = (fuse_fill_dir_t)(void *)address;
  const char *fn = env->GetStringUTFChars(name, 0);

  int ret = filler((void *)bufaddr, fn, NULL, 0);
  env->ReleaseStringUTFChars(name, fn);

  return ret;
}

#ifdef __cplusplus
}
#endif
