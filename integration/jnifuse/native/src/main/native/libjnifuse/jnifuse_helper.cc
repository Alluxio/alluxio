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

#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "debug.h"
#include "jnifuse_fs.h"
#include "jnifuse_impls.h"

#ifdef __cplusplus
extern "C" {
#endif

static struct fuse_operations jnifuse_oper;

JNIEXPORT jint JNICALL Java_alluxio_jnifuse_LibFuse_fuse_1main_1real(
    JNIEnv *env, jobject libfuseobj, jobject obj, jint jargc,
    jobjectArray jargv) {
  LOGI("Start initializing JNIFuse");
  LOGE("Validate standard errors can be logged as expected");
  jnifuse::JniFuseFileSystem::init(env, obj);

  int argc = jargc;
  LOGD("argc=%d", argc);
  char **argv = (char **)malloc(sizeof(char *) * argc);
  for (int i = 0; i < argc; i++) {
    jstring str = (jstring)env->GetObjectArrayElement(jargv, i);
    argv[i] = (char *)env->GetStringUTFChars(str, 0);
    LOGD("argv[%d]=%s", i, argv[i]);
  }

  jnifuse_oper.init = init_wrapper;
  jnifuse_oper.chmod = chmod_wrapper;
  jnifuse_oper.chown = chown_wrapper;
  jnifuse_oper.create = create_wrapper;
  jnifuse_oper.flush = flush_wrapper;
  jnifuse_oper.getattr = getattr_wrapper;
  jnifuse_oper.getxattr = getxattr_wrapper;
  jnifuse_oper.listxattr = listxattr_wrapper;
  jnifuse_oper.mkdir = mkdir_wrapper;
  jnifuse_oper.open = open_wrapper;
  jnifuse_oper.read = read_wrapper;
  jnifuse_oper.readdir = readdir_wrapper;
  jnifuse_oper.release = release_wrapper;
  jnifuse_oper.removexattr = removexattr_wrapper;
  jnifuse_oper.rename = rename_wrapper;
  jnifuse_oper.rmdir = rmdir_wrapper;
  jnifuse_oper.setxattr = setxattr_wrapper;
  jnifuse_oper.statfs = statfs_wrapper;
  jnifuse_oper.symlink = symlink_wrapper;
  jnifuse_oper.truncate = truncate_wrapper;
  jnifuse_oper.unlink = unlink_wrapper;
  jnifuse_oper.utimens = utimens_wrapper;
  jnifuse_oper.write = write_wrapper;

 // libfuse3: conn_info_opts can no longer be passed into fuse_main directly
 // for details, search for "The treatment of low-level options has been made more consistent" in 
 // https://github.com/libfuse/libfuse/blob/master/ChangeLog.rst#libfuse-300-2016-12-08
#if FUSE_USE_VERSION >= 30
  jnifuse_oper.init = init_wrapper;
  struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

  conn_info_opts = fuse_parse_conn_info_opts(&args);

  LOGD("%d", args.argc);

  int ret = fuse_main_real(args.argc, args.argv, &jnifuse_oper,
                           sizeof(struct fuse_operations), NULL);
#else
  int ret = fuse_main_real(argc, argv, &jnifuse_oper,
                           sizeof(struct fuse_operations), NULL);
#endif
  free(argv);
  LOGI("JNIFuse initialized");
  return ret;
}

jint JNICALL Java_alluxio_jnifuse_FuseFillDir_fill(JNIEnv *env, jclass cls,
                                                   jlong address, jlong bufaddr,
                                                   jstring name, jobject stbuf,
                                                   jlong off) {
  LOGD("enter fill");
  fuse_fill_dir_t filler = (fuse_fill_dir_t)(void *)address;
  const char *fn = env->GetStringUTFChars(name, 0);
  int ret;
#if FUSE_USE_VERSION >= 30
  if (stbuf) {
    ret = filler((void *)bufaddr, fn, NULL, 0, fuse_fill_dir_flags::FUSE_FILL_DIR_PLUS);
  } else {
    ret = filler((void *)bufaddr, fn, (struct stat*)stbuf, 0, fuse_fill_dir_flags::FUSE_FILL_DIR_PLUS);
  }
#else
  if (stbuf) {
    ret = filler((void *)bufaddr, fn, NULL, 0);
  } else {
    ret = filler((void *)bufaddr, fn, (struct stat*)stbuf, 0);
  }
#endif
  env->ReleaseStringUTFChars(name, fn);

  return ret;
}

jobject JNICALL Java_alluxio_jnifuse_LibFuse_fuse_1get_1context(JNIEnv *env, jobject obj) {
  LOGD("enter get_fuse_context");
  struct fuse_context *cxt = fuse_get_context();
  jobject fibuf =
      env->NewDirectByteBuffer((void *)cxt, sizeof(struct fuse_context));
  return fibuf;
}

jint JNICALL Java_alluxio_jnifuse_LibFuse_fuse_1get_1stat_1size(JNIEnv *env, jobject obj) {
  return sizeof(struct stat);
}

#ifdef __cplusplus
}
#endif
