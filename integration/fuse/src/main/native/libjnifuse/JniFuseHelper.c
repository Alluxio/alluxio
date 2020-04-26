#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#include "debug.h"
#include "alluxio_jnifuse_FuseFiller.h"
#include "alluxio_jnifuse_JniFuseFSBase.h"


static JavaVM* g_jvm;   // global jvm
static jobject g_jffs;    // global JniFuseFileSystem

static JNIEnv* get_env()
{
    JNIEnv* env;
    // (*global_jvm)->GetEnv(global_jvm, &env, JNI_VERSION_1_8);
    (*g_jvm)->AttachCurrentThreadAsDaemon(g_jvm, (void **)&env, NULL);
    return env;
}

static int getattr_wrapper(const char *path, struct stat* stbuf);
static int readdir_wrapper(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi);
static int open_wrapper(const char *path, struct fuse_file_info *fi);
static int read_wrapper(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

static struct fuse_operations opers = {
    .getattr = getattr_wrapper,
    .open = open_wrapper,
    .read = read_wrapper,
    .readdir = readdir_wrapper
};

jint JNICALL Java_alluxio_jnifuse_JniFuseFSBase_fuse_1main
  (JNIEnv *env, jobject obj, jint jargc, jobjectArray jargv)
{
    (*env)->GetJavaVM(env, &g_jvm);

    g_jffs = (*env)->NewGlobalRef(env, obj);
    (*env)->DeleteLocalRef(env, obj);

    int argc = jargc;
    LOGD("argc=%d\n", argc);

    char **argv = (char **)malloc(sizeof(char*) * argc);
    int i;
    for (i=0; i < argc; i++) {
        jstring str = (jstring)(*env)->GetObjectArrayElement(env, jargv, i);
        argv[i] = (char*)(*env)->GetStringUTFChars(env, str, 0);
        LOGD("argv[%d]=%s\n", i, argv[i]);
    }

    return fuse_main(argc, argv, &opers, NULL);

}

jint JNICALL Java_alluxio_jnifuse_FuseFiller_doFill
    (JNIEnv *env, jobject obj, jlong bufaddr, jstring name, jobject stbuf, jlong off)
{
    jclass clazz = (*env)->GetObjectClass(env, obj);
    jfieldID fieldid = (*env)->GetFieldID(env, clazz, "address", "J");
    fuse_fill_dir_t filler = (fuse_fill_dir_t) (*env)->GetLongField(env, obj, fieldid);
    const char* fn = (*env)->GetStringUTFChars(env, name, 0);

    return filler((void* )bufaddr, fn, NULL, 0);
}


static int getattr_wrapper(const char *path, struct stat* stbuf)
{
    LOGD("getattr %s\n", path);

    JNIEnv* env = get_env();

    jstring jspath = (*env)->NewStringUTF(env, path);
    size_t buflen = sizeof(struct stat);
    jobject buffer = (*env)->NewDirectByteBuffer(env, (void *)stbuf, buflen);

    jclass clazz = (*env)->GetObjectClass(env, g_jffs);
    static const char * signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
    jmethodID methodid = (*env)->GetMethodID(env, clazz, "getattrCallback", signature);
    int ret = (*env)->CallIntMethod(env, g_jffs, methodid, jspath, buffer);

    LOGD("file %s: mod=%d\n", path, stbuf->st_mode);

    return ret;
}

static int readdir_wrapper(const char* path, void* buf, fuse_fill_dir_t filler,
            off_t offset, struct fuse_file_info* fi)
{
    LOGD("C readdir: %s\n", path);

    JNIEnv* env = get_env();

    jstring jspath = (*env)->NewStringUTF(env, path);
    jclass fillerclazz = (*env)->FindClass(env, "com/eli/jnifuse/FuseFiller");
    jmethodID fillerconstructor = (*env)->GetMethodID(env, fillerclazz, "<init>", "(J)V");
    jobject fillerobj = (*env)->NewObject(env, fillerclazz, fillerconstructor, (void *)filler);
    jobject fibuf = (*env)->NewDirectByteBuffer(env, (void *)fi, sizeof(struct fuse_file_info));

    jclass clazz = (*env)->GetObjectClass(env, g_jffs);
    static const char* signature = "(Ljava/lang/String;JLcom/eli/jnifuse/FuseFiller;JLjava/nio/ByteBuffer;)I";
    jmethodID methodid = (*env)->GetMethodID(env, clazz, "readdirCallback", signature);
    int ret = (*env)->CallIntMethod(env, g_jffs,  methodid, jspath, buf, fillerobj, offset, fibuf);

    return ret;
}

static int open_wrapper(const char *path, struct fuse_file_info *fi)
{
    LOGD("C open: %s\n", path);

    JNIEnv* env = get_env();

    jstring jspath = (*env)->NewStringUTF(env, path);
    jobject fibuf = (*env)->NewDirectByteBuffer(env, (void *)fi, sizeof(struct fuse_file_info));

    jclass clazz = (*env)->GetObjectClass(env, g_jffs);
    static const char* signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
    jmethodID methodid = (*env)->GetMethodID(env, clazz, "openCallback", signature);
    int ret = (*env)->CallIntMethod(env, g_jffs,  methodid, jspath, fibuf);

    return ret;
}

static int read_wrapper(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
    LOGD("C read: %s\n", path);

    JNIEnv* env = get_env();

    jstring jspath = (*env)->NewStringUTF(env, path);
    jobject buffer = (*env)->NewDirectByteBuffer(env, (void *)buf, size);
    jobject fibuf = (*env)->NewDirectByteBuffer(env, (void *)fi, sizeof(struct fuse_file_info));

    jclass clazz = (*env)->GetObjectClass(env, g_jffs);
    static const char* signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;JJLjava/nio/ByteBuffer;)I";
    jmethodID methodid = (*env)->GetMethodID(env, clazz, "readCallback", signature);
    int ret = (*env)->CallIntMethod(env, g_jffs,  methodid, jspath, buffer, size, offset, fibuf);

    LOGD("C nread=%d\n", ret);

    return ret;
}