#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <jni.h>

#include "debug.h"
#include "JniFuseFileSystem.h"

#ifdef __cplusplus
extern "C" {
#endif

static int getattr_wrapper(const char *path, struct stat* stbuf)
{
  LOGD("getattr %s", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->getattrOper->call(path, stbuf);

  LOGD("file %s: size=%ld, mod=%d", path, stbuf->st_size, stbuf->st_mode);

  return ret;
}

static int open_wrapper(const char *path, struct fuse_file_info *fi)
{
  LOGD("open %s\n", path);

  int ret = jnifuse::JniFuseFileSystem::getInstance()->openOper->call(path, fi);

  return ret;
}

static int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
    LOGD("read: %s\n", path);

    int ret = jnifuse::JniFuseFileSystem::getInstance()->readOper->call(path, buf, size, offset, fi);

    LOGD("nread=%d\n", ret);

    return ret;
}

static int readdir_wrapper(const char* path, void* buf, fuse_fill_dir_t filler,
            off_t offset, struct fuse_file_info* fi)
{
    LOGD("readdir: %s\n", path);

    int ret = jnifuse::JniFuseFileSystem::getInstance()->readdirOper->call(path, buf, filler, offset, fi);

    return ret;
}

// TODO: Add more operations
// NOTE: 
static struct fuse_operations jnifuse_oper = {
  // .getattr = getattr_wrapper,
  // .open = open_wrapper,
  // .read = read_wrapper,
  // .readdir = readdir_wrapper
};

JNIEXPORT jint JNICALL Java_alluxio_jnifuse_LibFuse_fuse_1main_1real
  (JNIEnv *env, jobject libfuseobj, jobject obj, jint jargc, jobjectArray jargv)
{
  LOGD("enter fuse_main_real");

  jnifuse::JniFuseFileSystem* fs = jnifuse::JniFuseFileSystem::getInstance();
  fs->init(env, obj);

  int argc = jargc;
  LOGD("argc=%d", argc);

  char **argv = (char **)malloc(sizeof(char*) * argc);
  int i;
  for (i=0; i < argc; i++) {
    jstring str = (jstring)env->GetObjectArrayElement(jargv, i);
    argv[i] = (char*)env->GetStringUTFChars(str, 0);
    LOGD("argv[%d]=%s", i, argv[i]);
  }

  jnifuse_oper.getattr = getattr_wrapper;
  jnifuse_oper.open = open_wrapper;
  jnifuse_oper.read = read_wrapper;
  jnifuse_oper.readdir = readdir_wrapper;

  return fuse_main_real(argc, argv, &jnifuse_oper, sizeof(struct fuse_operations), NULL);
}

jint JNICALL Java_alluxio_jnifuse_FuseFillDir_fill
    (JNIEnv *env, jobject obj, jlong address, jlong bufaddr, jstring name, jobject stbuf, jlong off)
{
  LOGD("enter fill"); 
  fuse_fill_dir_t filler = (fuse_fill_dir_t) (void *)address;
  const char* fn = env->GetStringUTFChars(name, 0);

  return filler((void* )bufaddr, fn, NULL, 0);
}

#ifdef __cplusplus
}
#endif