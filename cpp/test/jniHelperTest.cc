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

#include <assert.h>
#include <string.h>
#include <iostream>

#include "jniHelper.h"

using ::jnihelper::JniHelper;

void TestJniStart() {
  JniHelper::Start();
  assert(JniHelper::GetJavaVM() != 0);
  assert(JniHelper::GetEnv() != 0);
}

jobject TestCreateObjectMethod() {
  jobject res = JniHelper::CreateObjectMethod("java/lang/Object");
  if (!res) {
    throw std::runtime_error("error when testing CreateObjectMethod()");
  }
  return res;
}

void TestCallIntMethod(jobject obj) {
  int res = JniHelper::CallIntMethod(obj, "java/lang/Object", "hashCode");
  assert(res >= 0);
}

void TestCallBooleanMethod(jobject obj) {
  bool res = JniHelper::CallBooleanMethod(obj, "java/lang/Object", "equals",
                                          obj);
  assert(res);
}

void TestCallStringMethod(jobject obj) {
  std::string res = JniHelper::CallStringMethod(obj, "java/lang/Object",
                                                "toString");
  assert(res.compare("") != 0);
}

void TestCallObjectMethod(jobject obj) {
  jobject classObj = JniHelper::CallObjectMethod(obj, "java/lang/Object",
                                                 "getClass",
                                                 "java/lang/Class");
  if (!classObj) {
    throw std::runtime_error("error when testing CallObjectMethod()");
  }
}

void TestCallVoidMethod(jobject obj) {
  JniHelper::CallVoidMethod(obj, "java/lang/Object", "finalize");
  Status status = JniHelper::AlluxioExceptionCheck();
  assert(status.ok());
}

void TestCallStaticObjectMethod() {
  jobject createFileOpt = JniHelper::CallStaticObjectMethod(
      "alluxio/client/file/options/CreateFileOptions", "defaults",
      "alluxio/client/file/options/CreateFileOptions");
  if (!createFileOpt) {
    throw std::runtime_error("error when testing CallStaticObjectMethod()");
  }
}

void TestGetClassName(jobject obj) {
  std::string name = JniHelper::GetObjectClassName(obj);
  assert(name.compare("java/lang/Object") == 0);
}

// Tests JNIHelper.h functions, the test function will call different types of
// java functions in jar file (the jar path is in ${CLASSPATH})
int main(void) {
  TestJniStart();
  jobject obj = TestCreateObjectMethod();
  TestCallIntMethod(obj);
  TestCallBooleanMethod(obj);
  TestCallStringMethod(obj);
  TestCallObjectMethod(obj);
  TestCallVoidMethod(obj);
  TestCallStaticObjectMethod();
  TestGetClassName(obj);
  return 0;
}
