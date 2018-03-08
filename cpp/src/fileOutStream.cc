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

#include "fileOutStream.h"

#include <stdio.h>
#include <cstring>

using ::alluxio::FileOutStream;

FileOutStream::FileOutStream(jobject alluxioOutStream) {
  FileOutStream::outStream = alluxioOutStream;
}

FileOutStream::~FileOutStream() {
}

Status FileOutStream::Write(char b) {
  return FileOutStream::Write(&b, 0, 1);
}

Status FileOutStream::Write(const char* buf, size_t off, size_t len) {
  try {
    int byteLen = strlen(buf);
    JNIEnv* env = JniHelper::GetEnv();
    jbyteArray jByteArrays = env->NewByteArray(byteLen);
    char* tmpBuf = const_cast<char*>(buf);
    env->SetByteArrayRegion(jByteArrays, 0, byteLen,
                            reinterpret_cast<jbyte*>(tmpBuf));
    JniHelper::CallVoidMethod(FileOutStream::outStream,
                              "alluxio/client/file/FileOutStream", "write",
                              jByteArrays, static_cast<int>(off),
                              static_cast<int>(len));
    env->DeleteLocalRef(jByteArrays);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileOutStream::Flush() {
  try {
    JniHelper::CallVoidMethod(FileOutStream::outStream,
         "alluxio/client/file/FileOutStream", "flush");
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileOutStream::Cancel() {
  try {
    JniHelper::CallVoidMethod(FileOutStream::outStream,
                              "alluxio/client/file/FileOutStream", "cancel");
    JniHelper::DeleteObjectRef(FileOutStream::outStream);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileOutStream::Close() {
  try {
    JniHelper::CallVoidMethod(FileOutStream::outStream,
                              "alluxio/client/file/FileOutStream", "close");
    JniHelper::DeleteObjectRef(FileOutStream::outStream);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}
