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

#include <wire.h>
// #include <stdio.h>

using ::alluxio::Mode;
using ::alluxio::Bits;
using ::jnihelper::JniHelper;


Bits* Mode::getOwnerBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getOwnerBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setOwnerBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setOwnerBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}

Bits* Mode::getGroupBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getGroupBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setGroupBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setGroupBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}

Bits* Mode::getOtherBits() {
  jobject jBits = JniHelper::CallObjectMethod(
      options, "alluxio/security/authorization/Mode",
      "getOtherBits", "alluxio/security/authorization/Mode$Bits");
  std::string str = JniHelper::CallStringMethod(
      jBits, "alluxio/security/authorization/Mode$Bits", "toString");
  JniHelper::DeleteObjectRef(jBits);
  return new Bits(str);
}

void Mode::setOtherBits(Bits* bits) {
  jobject jBits = bits->tojBits();
  JniHelper::CallVoidMethod(options, "alluxio/security/authorization/Mode",
                            "setOtherBits", jBits);
  JniHelper::DeleteObjectRef(jBits);
}
