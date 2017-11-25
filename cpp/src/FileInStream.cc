/**
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#include <cmath>

#include "FileInStream.h"

using namespace alluxio;

FileInStream::FileInStream(jobject alluxioInStream) {
  FileInStream::inStream = alluxioInStream;
}

FileInStream::~FileInStream() {
}

Status FileInStream::Read(char* b) {
  return FileInStream::Read(b, 0, strlen(b), NULL);
}

Status FileInStream::Read(char* buf, size_t off, size_t len,
             size_t* result) {
  try {
    JNIEnv *env = JniHelper::GetEnv();
    jbyteArray jByteArrays = env->NewByteArray(len + off);
    env->SetByteArrayRegion(jByteArrays, off, len, (jbyte*)buf);
    size_t res = JniHelper::CallIntMethod(FileInStream::inStream,
                                          "alluxio/client/file/FileInStream",
                                          "read", jByteArrays, (int)off,
                                          (int)len);
    if (res == (pow(2, 64) - 1)) {
      res = 0;
    }
    *result = res;
    if (res > 0) {
      env->GetByteArrayRegion(jByteArrays, 0, res, (jbyte*)buf);
    }
    env->DeleteLocalRef(jByteArrays);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileInStream::Seek(size_t pos) {
  try {
    JniHelper::CallVoidMethod(FileInStream::inStream,
                              "alluxio/client/file/FileInStream",
                              "seek", (long)pos);
    return JniHelper::AlluxioExceptionCheck( );
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileInStream::Skip(size_t pos) {
  try {
    JniHelper::CallVoidMethod(FileInStream::inStream,
                              "alluxio/client/file/FileInStream", "skip",
                              (long)pos);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileInStream::Close() {
  JniHelper::DeleteObjectRef(FileInStream::inStream);
  return Status::OK();
}
