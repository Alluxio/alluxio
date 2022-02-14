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
#include <pthread.h>

#include "check.h"
#include "debug.h"

namespace jnifuse {

static JavaVM* g_jvm = nullptr;

// Key for per-thread JNIEnv* data.  Non-NULL in threads attached to `g_jvm` by
// AttachCurrentThreadIfNeeded(), NULL in unattached threads and threads that
// were attached by the JVM because of a Java->native call.
static pthread_key_t g_jni_ptr;

JavaVM* GetJVM() {
  JNIFUSE_CHECK(g_jvm, "JNI_OnLoad failed to run?");
  return g_jvm;
}

// Return a |JNIEnv*| usable on this thread or NULL if this thread is detached.
JNIEnv* GetEnv() {
  void* env = nullptr;
  // TODO(lu) change to store the value somewhere
  jint status = g_jvm->GetEnv(&env, JNI_VERSION_1_8);
  JNIFUSE_CHECK(((env != nullptr) && (status == JNI_OK)) || ((env == nullptr) && (status == JNI_EDETACHED)),
            "Unexpected GetEnv return: %d", status);
  return reinterpret_cast<JNIEnv*>(env);
}

static void ThreadDestructor(void* prev_jni_ptr) {
  // This function only runs on threads where `g_jni_ptr` is non-NULL, meaning
  // we were responsible for originally attaching the thread, so are responsible
  // for detaching it now.  However, because some JVM implementations (notably
  // Oracle's http://goo.gl/eHApYT) also use the pthread_key_create mechanism,
  // the JVMs accounting info for this thread may already be wiped out by the
  // time this is called. Thus it may appear we are already detached even though
  // it was our responsibility to detach!  Oh well.
  if (!GetEnv())
    return;

  JNIFUSE_CHECK(GetEnv() == prev_jni_ptr, 
      "Detaching from another thread");
  jint status = g_jvm->DetachCurrentThread();
  JNIFUSE_CHECK(status == JNI_OK, "Failed to detach thread: %d", status);
  JNIFUSE_CHECK(!GetEnv(), "Detaching was a successful no-op???");
}

jint InitGlobalJniVariables(JavaVM* jvm) {
  JNIFUSE_CHECK(!g_jvm, "Cannot initialize global jni variables more than once");
  g_jvm = jvm;
  JNIFUSE_CHECK(g_jvm, "InitGlobalJniVariables handed NULL JVM");

  JNIFUSE_CHECK(!pthread_key_create(&g_jni_ptr, &ThreadDestructor),
      "Failed in pthread_key_create");

  JNIEnv* jni = nullptr;
  jint status = jvm->GetEnv(reinterpret_cast<void**>(&jni), JNI_VERSION_1_8);
  if (status != JNI_OK) {
    LOGD("Failed to get env with JNI_VERSION_1_8, status is %d", status);
    if (status == JNI_EDETACHED) {
      status = jvm->AttachCurrentThread(reinterpret_cast<void**>(&jni), NULL);
    }
  }  
  if (status != JNI_OK) {
    return JNI_ERR;
  }
  // TODO(lu) validate if this works well with java 11
  return JNI_VERSION_1_8;
}

// Return a |JNIEnv*| usable on this thread.  Attaches to `g_jvm` if necessary.
JNIEnv* AttachCurrentThreadIfNeeded() {
    JNIEnv* jni = GetEnv();
    if (jni) {
      return jni;
    }
    JNIFUSE_CHECK(!pthread_getspecific(g_jni_ptr),
        "This thread has a JNIEnv* but not attached?");
    JNIEnv* env = nullptr;
    // TODO(lu) set args
    // TODO(lu) attach as deomon?
    // TODO(lu) improve error logging with condition and error code
    JNIFUSE_CHECK(!g_jvm->AttachCurrentThreadAsDaemon((void **)&env, nullptr), "Failed to attach thread");
    JNIFUSE_CHECK(env, "AttachCurrentThread handed back NULL!");
    jni = reinterpret_cast<JNIEnv*>(env);
    JNIFUSE_CHECK(!pthread_setspecific(g_jni_ptr, jni), "pthread_setspecific");
    return jni;
}
}  // namespace jnifuse
