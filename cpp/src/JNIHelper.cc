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

#include "JNIHelper.h"

using namespace jnihelper;

static pthread_key_t g_key;

void _detachCurrentThread(void* a) {
  JniHelper::GetJavaVM()->DetachCurrentThread();
}

JavaVM* JniHelper::_psJavaVM = nullptr;

JavaVM* JniHelper::GetJavaVM() {
  pthread_t thisthread = pthread_self();
  return _psJavaVM;
}

void JniHelper::SetJavaVM(JavaVM *javaVM) {
  pthread_t thisthread = pthread_self();
  _psJavaVM = javaVM;
  pthread_key_create(&g_key, _detachCurrentThread);
}

JNIEnv* JniHelper::CacheEnv(JavaVM* jvm) {
  JNIEnv* _env = nullptr;
  jint ret = jvm->GetEnv((void**)&_env, JNI_VERSION_1_8);
  switch (ret) {
  case JNI_OK :
    pthread_setspecific(g_key, _env);
    return _env;
  case JNI_EDETACHED :
    if (jvm->AttachCurrentThread((void**)_env, nullptr) < 0) {
      return nullptr;
    } else {
      pthread_setspecific(g_key, _env);
      return _env;
    }
  case JNI_EVERSION :
  // Cannot recover from this error
  default :
    return nullptr;
  }
}

JNIEnv* JniHelper::GetEnv() {
  JNIEnv* _env = (JNIEnv* )pthread_getspecific(g_key);
  if (_env == nullptr) {
    _env = JniHelper::CacheEnv(_psJavaVM);
  }
  return _env;
}

bool JniHelper::GetMethodInfo(JniMethodInfo &methodinfo, const char *className,
                              const char *methodName, const char *paramCode,
                              bool isStatic) {
  if ((nullptr == className) || (nullptr == methodName) ||
      (nullptr == paramCode)) {
    return false;
  }
  JNIEnv* env = JniHelper::GetEnv();
  if (! env) {
    return false;
  }
  jclass classID = ClassCache::instance(env)->getJclass(className);
  if (! classID) {
    env->ExceptionClear();
    return false;
  }
  jmethodID methodID;
  if(isStatic) {
    methodID = env->GetStaticMethodID(classID, methodName, paramCode);
  } else {
    methodID = env->GetMethodID(classID, methodName, paramCode);
  }
  if (! methodID) {
    env->ExceptionClear();
    return false;
  }

  methodinfo.classID = classID;
  methodinfo.env = env;
  methodinfo.methodID = methodID;
  return true;
}

jstring JniHelper::SringToJstring(JNIEnv* env, const char* pat) {
  jclass strClass = env->FindClass("Ljava/lang/String;");
  jmethodID ctorID = env->GetMethodID(strClass, "<init>",
                                      "([BLjava/lang/String;)V");
  jbyteArray bytes = env->NewByteArray(strlen(pat));
  env->SetByteArrayRegion(bytes, 0, strlen(pat), (jbyte*)pat);
  jstring encoding = env->NewStringUTF("GB2312");
  return (jstring)env->NewObject(strClass, ctorID, bytes, encoding);
}

jstring JniHelper::Convert(LocalRefMapType& localRefs, JniMethodInfo& t,
                           const char* x) {
  jstring ret =JniHelper::SringToJstring(t.env, x);
  localRefs[t.env].push_back(ret);
  return ret;
}

jstring JniHelper::Convert(LocalRefMapType& localRefs, JniMethodInfo& t,
                           const std::string& x) {
  jstring ret =JniHelper::SringToJstring(t.env, x.c_str());
  localRefs[t.env].push_back(ret);
  return ret;
}

void JniHelper::ReportError(const std::string& className,
                            const std::string& methodName,
                            const std::string& signature) {
  std::string errorMsg = "Failed to call java method. Class name: ";
  errorMsg = errorMsg + className + ", method name: " + methodName +
      ", signature: " + signature;
  throw errorMsg;
}

std::string ClassCache::getClassName(jobject obj) {
  std::map<jobject, const std::string>::iterator itr =
      objectToTypeNameMap.find(obj);
  if (itr != objectToTypeNameMap.end()) {
    return (std::string)itr->second;
  } else {
    const std::string className = JniHelper::GetObjectClassName(obj);
    return "L" + className + ";";
  }
}

ClassCache* ClassCache::instance(JNIEnv* env) {
  return ClassCaches::getInstance().getCache(env);
}

void ClassCache::CacheClassName(jobject obj, const std::string& classname) {
  objectToTypeNameMap.insert(std::make_pair(obj,  "L" +classname+ ";"));
}

void ClassCache::deleteClassName(jobject obj) {
  objectToTypeNameMap.erase(obj);
}

jclass ClassCache::getJclass(const char* className) {
  std::map<const char*, jclass>::iterator it =
      classNameToJclassMap.find(className);
  if (it != classNameToJclassMap.end()) {
    return (jclass)classNameToJclassMap[className];
  }
  // Cache miss, finds the class and put into cache
  jclass globalCls = NULL;
  try {
    std::string tempStr(className);
    jclass cls = env->FindClass(className);
    // Handles inner class
    if(tempStr.find("$")){
      return cls;
    }
    // Finds in env, first create a global reference (otherwise, the reference
    // will be can be dangling), then update cache
    globalCls = (jclass)env->NewGlobalRef(cls);
    env->DeleteLocalRef(cls); // delete local reference
    classNameToJclassMap.insert(std::make_pair(className, globalCls));
    return cls;
  } catch (...) {
    if (globalCls != NULL) {
      env->DeleteGlobalRef(globalCls);
    }
    throw;
  }
}

ClassCache::~ClassCache() {
  JNIEnv* env = JniHelper::GetEnv();
  std::map<const char*, jclass>::iterator it;
  for (it = classNameToJclassMap.begin(); it != classNameToJclassMap.end();
      it ++) {
    jclass cls = (jclass)it->second;
    if (cls != NULL) {
      env->DeleteGlobalRef(cls);
    }
  }
  std::map<jobject, const std::string>::iterator iter;
  for (iter = objectToTypeNameMap.begin(); iter != objectToTypeNameMap.end();
      iter ++) {
    jobject cls = (jobject)iter->first;
    if (cls != NULL) {
      env->DeleteGlobalRef(cls);
    }
  }
  objectToTypeNameMap.clear();
}

ClassCache* ClassCaches::getCache(JNIEnv *env) {
  std::map<JNIEnv *, std::shared_ptr<ClassCache> >::iterator it =
      m_caches.find(env);
  if (it != m_caches.end()) {
    return it->second.get();
  }
  std::shared_ptr<ClassCache> cache(new ClassCache(env));
  m_caches.insert(std::make_pair(env, cache));
  return cache.get();
}
