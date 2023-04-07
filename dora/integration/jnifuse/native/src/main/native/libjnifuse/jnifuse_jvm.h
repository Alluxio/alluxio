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

#ifndef FUSE_NATIVE_LIBJNIFUSE_JVM_H_
#define FUSE_NATIVE_LIBJNIFUSE_JVM_H_

#include <jni.h>

namespace jnifuse {

jint InitGlobalJniVariables(JavaVM* jvm);

// Return a |JNIEnv*| usable on this thread or NULL if this thread is detached.
JNIEnv* GetEnv();

JavaVM* GetJVM();

// Return a |JNIEnv*| usable on this thread.  Attaches to JVM if necessary.
JNIEnv* AttachCurrentThreadIfNeeded();

}  // namespace jnifuse

#endif  // FUSE_NATIVE_LIBJNIFUSE_JVM_H_
