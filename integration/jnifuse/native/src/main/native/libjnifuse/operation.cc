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

#include "operation.h"

#include "debug.h"

namespace jnifuse {

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

GetxattrOperation::GetxattrOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "getxattrCallback", signature);
}

int GetxattrOperation::call(const char *path, const char *name, char *value, size_t size) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jstring jsname = env->NewStringUTF(name);
  jobject buffer = env->NewDirectByteBuffer((void *)value, size);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, jsname, buffer);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(jsname);
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
      "(Ljava/lang/String;JJJLjava/nio/"
      "ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "readdirCallback", signature);
}

int ReaddirOperation::call(const char *path, void *buf, fuse_fill_dir_t filler,
                           off_t offset, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buf,
                               (void *)filler, offset, fibuf);
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

ListxattrOperation::ListxattrOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "listxattrCallback", signature);
}

int ListxattrOperation::call(const char *path, char *list, size_t size) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  int a = 2;
  a++;
  jobject buffer = env->NewDirectByteBuffer((void *)list, size * sizeof(char));
  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buffer);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(buffer);

  return ret;
}

ChmodOperation::ChmodOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;J)I";
  this->methodID = env->GetMethodID(this->clazz, "chmodCallback", signature);
}

int ChmodOperation::call(const char *path, mode_t mode) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, mode);

  env->DeleteLocalRef(jspath);

  return ret;
}

ChownOperation::ChownOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;JJ)I";
  this->methodID = env->GetMethodID(this->clazz, "chownCallback", signature);
}

int ChownOperation::call(const char *path, uid_t uid, gid_t gid) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, uid, gid);

  env->DeleteLocalRef(jspath);

  return ret;
}

CreateOperation::CreateOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;JLjava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "createCallback", signature);
}

int CreateOperation::call(const char *path, mode_t mode,
                          struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, mode, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(fibuf);

  return ret;
}

MkdirOperation::MkdirOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;J)I";
  this->methodID = env->GetMethodID(this->clazz, "mkdirCallback", signature);
}

int MkdirOperation::call(const char *path, mode_t mode) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, mode);

  env->DeleteLocalRef(jspath);

  return ret;
}

RmdirOperation::RmdirOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;)I";
  this->methodID = env->GetMethodID(this->clazz, "rmdirCallback", signature);
}

int RmdirOperation::call(const char *path) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath);

  env->DeleteLocalRef(jspath);

  return ret;
}

WriteOperation::WriteOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature =
      "(Ljava/lang/String;Ljava/nio/ByteBuffer;JJLjava/nio/ByteBuffer;)I";
  this->methodID = env->GetMethodID(this->clazz, "writeCallback", signature);
}

int WriteOperation::call(const char *path, const char *buf, size_t size,
                         off_t off, struct fuse_file_info *fi) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jobject buffer = env->NewDirectByteBuffer((void *)buf, size);
  jobject fibuf =
      env->NewDirectByteBuffer((void *)fi, sizeof(struct fuse_file_info));

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buffer, size,
                               off, fibuf);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(buffer);
  env->DeleteLocalRef(fibuf);

  return ret;
}

RenameOperation::RenameOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/lang/String;)I";
  this->methodID = env->GetMethodID(this->clazz, "renameCallback", signature);
}

int RenameOperation::call(const char *oldPath, const char *newPath) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(oldPath);
  jstring jspathNew = env->NewStringUTF(newPath);

   int ret = env->CallIntMethod(this->obj, this->methodID, jspath, jspathNew);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(jspathNew);

  return ret;
}

RemovexattrOperation::RemovexattrOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/lang/String;)I";
  this->methodID = env->GetMethodID(this->clazz, "removexattrCallback", signature);
}

int RemovexattrOperation::call(const char *path, const char *list) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jstring jslist = env->NewStringUTF(list);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, jslist);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(jslist);

  return ret;
}

SetxattrOperation::SetxattrOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/nio/ByteBuffer;JI)I";
  this->methodID = env->GetMethodID(this->clazz, "setxattrCallback", signature);
}

int SetxattrOperation::call(const char *path, const char *name, const char *value,
                            size_t size, int flags) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);
  jstring jsname = env->NewStringUTF(name);
  jobject buffer = env->NewDirectByteBuffer((void *)value, size);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, buffer, size, flags);

  env->DeleteLocalRef(jspath);
  env->DeleteLocalRef(jsname);
  env->DeleteLocalRef(buffer);

  return ret;
}

SymlinkOperation::SymlinkOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;Ljava/lang/String;)I";
  this->methodID = env->GetMethodID(this->clazz, "symlinkCallback", signature);
}

int SymlinkOperation::call(const char *linkname, const char *path) {
  JNIEnv *env = this->fs->getEnv();
  jstring jlinkname = env->NewStringUTF(linkname);
  jstring jpath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jlinkname, jpath);

  env->DeleteLocalRef(jlinkname);
  env->DeleteLocalRef(jpath);

  return ret;
}

TruncateOperation::TruncateOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;J)I";
  this->methodID = env->GetMethodID(this->clazz, "truncateCallback", signature);
}

int TruncateOperation::call(const char *path, off_t size) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, size);

  env->DeleteLocalRef(jspath);

  return ret;
}

UtimensOperation::UtimensOperation(JniFuseFileSystem *fs) {
  this->fs = fs;
  JNIEnv *env = this->fs->getEnv();
  this->obj = this->fs->getFSObj();
  this->clazz = env->GetObjectClass(this->fs->getFSObj());
  this->signature = "(Ljava/lang/String;JJJJ)I";
  this->methodID = env->GetMethodID(this->clazz, "utimensCallback", signature);
}

int UtimensOperation::call(const char *path, const struct timespec ts[2]) {
  JNIEnv *env = this->fs->getEnv();
  jstring jspath = env->NewStringUTF(path);

  int ret = env->CallIntMethod(this->obj, this->methodID, jspath, ts[0].tv_sec, ts[0].tv_nsec, ts[1].tv_sec, ts[1].tv_nsec);

  env->DeleteLocalRef(jspath);

  return ret;
}

}  // namespace jnifuse
