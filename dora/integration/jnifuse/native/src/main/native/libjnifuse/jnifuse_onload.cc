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

#include <jni.h>

#include "debug.h"
#include "jnifuse_jvm.h"

namespace jnifuse {
extern "C" jint JNIEXPORT JNICALL JNI_OnLoad(JavaVM* jvm, void* reserved) {
  LOGD("Start loading libjnifuse");
  jint ret = InitGlobalJniVariables(jvm);
  if (ret < 0) {
    LOGE("Failed to load libjnifuse");
    return -1;
  }
  LOGI("Loaded libjnifuse");
  return ret;
}

extern "C" void JNIEXPORT JNICALL JNI_OnUnLoad(JavaVM* jvm, void* reserved) {
}
} // namespace jnifuse
