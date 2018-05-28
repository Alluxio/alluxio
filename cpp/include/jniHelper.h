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

#ifndef CPP_INCLUDE_JNIHELPER_H_
#define CPP_INCLUDE_JNIHELPER_H_

#include <status.h>
#include <stdio.h>
#include <jni.h>
#include <pthread.h>
#include <functional>
#include <memory>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <map>
#include <string>

namespace jnihelper {
// Caches the jclass object and Class name for further JNI calling
class ClassCache {
 public:
  ~ClassCache();
  // Gets the static ClassCache Instance
  explicit ClassCache(JNIEnv* jniEnv) : env(jniEnv) {}
  static ClassCache* instance(JNIEnv* env);
  // Gets jclass object by class name
  jclass getJclass(const char* className);
  // Stores the map from object to its class name
  void CacheClassName(jobject obj, const std::string& classname);
  // Deletes item in objectToTypeNameMap
  void deleteClassName(jobject obj);
  // Gets class name of object
  std::string getClassName(jobject obj);
 private:
  std::map<const char*, jclass>  classNameToJclassMap;
  std::map<jobject, const std::string> objectToTypeNameMap;
  JNIEnv* env;
};

// Caches the map from JNIEnv* to ClassCache instance
class ClassCaches {
 public:
  static ClassCaches& getInstance() {
    static ClassCaches instance;
    return instance;
  }
  ClassCache* getCache(JNIEnv *env);

 private:
  std::map<JNIEnv*, std::shared_ptr<ClassCache>> m_caches;
};

typedef struct JniMethodInfo_ {
  JNIEnv* env;
  jclass classID;
  jmethodID methodID;
} JniMethodInfo;

// The JNI function tools. The public static function can be called when calling
// JNI function.
class JniHelper {
 public:
  typedef std::map<JNIEnv*, std::vector<jobject>> LocalRefMapType;
  // Sets jvm information for JNI calling
  static void SetJavaVM(JavaVM *javaVM);
  // Gets the object of JavaVM
  static JavaVM* GetJavaVM();
  // Gets JNIEnv object of current thread, creates if not set
  static JNIEnv* GetEnv();
  // Sets JNI environment. Must be called once before JNI operations. The
  // CLASSPATH environment must have the alluxio client jar file path. See
  // README.md
  static void Start() {
    JNIEnv *env;
    JavaVM *jvm;
    JavaVMOption options[1];
    JavaVMInitArgs vm_args;

    char *classpath = getenv("CLASSPATH");
    if (classpath == NULL) {
      throw std::runtime_error("CLASSPATH env variable is not been set");
    }

    const char *classpath_opt = "-Djava.class.path=";
    std::string classpathString(classpath_opt);
    classpathString.append(classpath);

    options[0].optionString = const_cast<char *>(classpathString.c_str());
    memset(&vm_args, 0, sizeof(vm_args));
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = 1;
    vm_args.options = options;

    JNI_CreateJavaVM(&jvm, reinterpret_cast<void**>(&env), &vm_args);
    JniHelper::SetJavaVM(jvm);
    JniHelper::CacheEnv(jvm);
  }

  template <typename... Ts>
  static void CallVoidMethod(jobject obj, const std::string& className,
                             const std::string& methodName, Ts... xs) {
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")V";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      t.env->CallVoidMethod(obj, t.methodID, Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
  }

  template <typename... Ts>
  static void CallStaticVoidMethod(const std::string& className,
                                   const std::string& methodName, Ts... xs) {
  JniMethodInfo t;
  std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")V";
  if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                               signature.c_str(), true)) {
    LocalRefMapType localRefs;
    t.env->CallStaticVoidMethod(t.classID, t.methodID,
                                Convert(&localRefs, &t, xs)...);
    DeleteLocalRefs(t.env, &localRefs);
  } else {
    ReportError(className, methodName, signature);
  }
}

  template <typename... Ts>
  static jobject CallObjectMethod(jobject obj, const std::string& className,
                                  const std::string& methodName,
                                  const std::string& returnClassName,
                                  Ts... xs) {
    jobject res;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...));
    // if the returned object is array
    if (returnClassName[0] != '[') {
      signature = signature + ")L" + returnClassName + ";";
    } else {
      signature = signature + ")" + returnClassName + ";";
    }
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      res = t.env->CallObjectMethod(obj, t.methodID,
                                    Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
      return NULL;
    }
    ClassCache::instance(t.env)->CacheClassName(res, returnClassName);
    return res;
  }

  template <typename... Ts>
  static jobject CreateObjectMethod(const std::string& className, Ts... xs) {
    jobject res;
    JniMethodInfo t;
    std::string methodName = "<init>";
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")V";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      res = t.env->NewObject(t.classID, t.methodID,
                             Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
      return 0;
    }
    ClassCache::instance(t.env)->CacheClassName(res, className);
    return res;
  }

  template <typename... Ts>
  static bool CallBooleanMethod(jobject obj, const std::string& className,
                                const std::string& methodName, Ts... xs) {
    jboolean jret = JNI_FALSE;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")Z";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      jret = t.env->CallBooleanMethod(obj, t.methodID,
                                      Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    return (jret == JNI_TRUE);
  }

  template <typename... Ts>
  static int CallIntMethod(jobject obj, const std::string& className,
                           const std::string& methodName, Ts... xs) {
    jint res;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")I";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      res = t.env->CallIntMethod(obj, t.methodID,
                                 Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    return static_cast<int>(res);
  }

  template <typename... Ts>
  static int64_t CallLongMethod(jobject obj, const std::string& className,
                           const std::string& methodName, Ts... xs) {
    jlong res;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")J";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      res = t.env->CallLongMethod(obj, t.methodID,
                                 Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    return static_cast<int64_t>(res);
  }

  template <typename... Ts>
  static int64_t CallStaticLongMethod(const std::string& className,
                                      const std::string& methodName,
                                      Ts... xs) {
    jlong res;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...)) + ")J";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), true)) {
      LocalRefMapType localRefs;
      res = t.env->CallStaticLongMethod(t.classID, t.methodID,
                                        Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    return static_cast<int64_t>(res);
  }

  template <typename... Ts>
  static std::string CallStringMethod(jobject obj, const std::string& className,
                                      const std::string& methodName, Ts... xs) {
    std::string res;
    JniMethodInfo t;
    std::string signature =
        "(" + std::string(GetJniSignature(xs...)) + ")Ljava/lang/String;";
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), false)) {
      LocalRefMapType localRefs;
      jstring jret = (jstring)t.env->CallObjectMethod(obj, t.methodID,
                                         Convert(&localRefs, &t, xs)...);
      res = JniHelper::JstringToString(jret);
      DeleteObjectRef(jret);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    return res;
  }

  template <typename... Ts>
  static jobject CallStaticObjectMethod(const std::string& className,
                                        const std::string& methodName,
                                        const std::string& returnClassName,
                                        Ts... xs) {
    jobject res;
    JniMethodInfo t;
    std::string signature = "(" + std::string(GetJniSignature(xs...));
    // if the returned object is array
    if (returnClassName[0] != '[') {
      signature = signature + ")L" + returnClassName + ";";
    } else {
      signature = signature + ")" + returnClassName + ";";
    }
    if (JniHelper::GetMethodInfo(&t, className.c_str(), methodName.c_str(),
                                 signature.c_str(), true)) {
      LocalRefMapType localRefs;
      res = t.env->CallStaticObjectMethod(t.classID, t.methodID,
                                          Convert(&localRefs, &t, xs)...);
      DeleteLocalRefs(t.env, &localRefs);
    } else {
      ReportError(className, methodName, signature);
    }
    ClassCache::instance(t.env)->CacheClassName(res, returnClassName);
    return res;
  }

  // Gets class name by jobject instance
  static std::string GetObjectClassName(jobject obj) {
    jobject classObj = JniHelper::CallObjectMethod(obj, "java/lang/Object",
                                                   "getClass",
                                                   "java/lang/Class");
    std::string className = JniHelper::CallStringMethod(classObj,
                                                        "java/lang/Class",
                                                        "getName");
    DeleteObjectRef(classObj);
    for (int i = 0; i < className.length(); i ++) {
      if (className[i] == '.') {
        className[i] = '/';
      }
    }
    return className;
  }

  static void DeleteLocalRefs(JNIEnv* env, LocalRefMapType* localRefs) {
    if (!env) {
     return;
    }
    for (const auto& ref : (*localRefs)[env]) {
      env->DeleteLocalRef(ref);
      ClassCache::instance(env)->deleteClassName(ref);
    }
    (*localRefs)[env].clear();
  }

  static void DeleteObjectRef(jobject obj) {
    JNIEnv* env = GetEnv();
    ClassCache::instance(env)->deleteClassName(obj);
    env->DeleteLocalRef(obj);
  }

  // Get Status instance by status name
  static Status GetStatusFromAlluxioException(const std::string& statusName,
      const std::string& errorMsg) {
    if (statusName.compare("ABORTED") == 0) {
      return Status::aborted(errorMsg);
    } else if (statusName.compare("ALREADY_EXISTS") == 0) {
      return Status::alreadyExist(errorMsg);
    } else if (statusName.compare("CANCELED") == 0) {
      return Status::canceled(errorMsg);
    } else if (statusName.compare("DATA_LOSS") == 0) {
      return Status::dataLoss(errorMsg);
    } else if (statusName.compare("DEADLINE_EXCEEDED") == 0) {
      return Status::deadlineExceeded(errorMsg);
    } else if (statusName.compare("FAILED_PRECONDITION") == 0) {
      return Status::failedPrecondition(errorMsg);
    } else if (statusName.compare("INTERNAL") == 0) {
      return Status::internal(errorMsg);
    } else if (statusName.compare("INVALID_ARGUMENT") == 0) {
      return Status::invalidArgument(errorMsg);
    } else if (statusName.compare("NOT_FOUND") == 0) {
      return Status::notFound(errorMsg);
    } else if (statusName.compare("OUT_OF_RANGE") == 0) {
      return Status::outOfRange(errorMsg);
    } else if (statusName.compare("PERMISSION_DENIED") == 0) {
      return Status::permissionDenied(errorMsg);
    } else if (statusName.compare("RESOURCE_EXHAUSTED") == 0) {
      return Status::resourceExhausted(errorMsg);
    } else if (statusName.compare("UNAUTHENTICATED") == 0) {
      return Status::unAuthenticated(errorMsg);
    } else if (statusName.compare("UNAVAILABLE") == 0) {
      return Status::unavailable(errorMsg);
    } else if (statusName.compare("UNIMPLEMENTED") == 0) {
      return Status::unImplemented(errorMsg);
    } else if (statusName.compare("UNKNOWN") == 0) {
      return Status::unknown(errorMsg);
    }
    return Status::OK();
  }

  // Checks if there are some AlluxioExceptions happening during JNI calling.
  // Returns Status object depending on the AlluxioException status
  static Status AlluxioExceptionCheck() {
    JNIEnv *env = GetEnv();
    jthrowable javaException;
    javaException = env->ExceptionOccurred();
    jboolean error = env->ExceptionCheck();

    if (error) {
      env->ExceptionDescribe();
      env->ExceptionClear();
      std::string exceptionName = JniHelper::GetObjectClassName(
                                                 (jobject)javaException);
      std::string errorMsg = JniHelper::CallStringMethod((jobject)javaException,
                                                         "java/lang/Throwable",
                                                         "getMessage");
      char* base = NULL;
      char* m = NULL;
      memcpy(m, exceptionName.c_str(), exceptionName.length() + 1);
      char* exceptionSplit =
          strtok_r(m, "/", &base);
      if (strcmp(exceptionSplit, "java") == 0) {
        // these exceptions are thrown by FileInStream and FileOutStream, are
        // not the subClass of alluxio.exception.AlluxioException
        return Status::internal(errorMsg);
      }
      char *save = NULL;
      for (int i = 2; i > 0; i --) {
        exceptionSplit = strtok_r(NULL, "/", &save);
      }

      jobject statusInAlluxio;
      // handles the AlluxioStatusException directly
      if (strcmp(exceptionSplit, "Status") == 0) {
        statusInAlluxio = (jobject)javaException;
      } else {  // Converts the AlluxioException to AlluxioStatusException
        ClassCache::instance(env)->CacheClassName((jobject)javaException,
            "alluxio/exception/AlluxioException");
        jobject statusException = JniHelper::CallStaticObjectMethod(
            "alluxio/exception/status/AlluxioStatusException",
            "fromAlluxioException",
            "alluxio/exception/status/AlluxioStatusException",
            (jobject)javaException);
        statusInAlluxio = JniHelper::CallObjectMethod(statusException,
            "alluxio/exception/status/AlluxioStatusException", "getStatus",
            "alluxio/exception/status/Status");
        DeleteObjectRef(statusException);
      }
      std::string statusName = JniHelper::CallStringMethod(
          (jobject)statusInAlluxio, "java/lang/Enum", "name");

      DeleteObjectRef(statusInAlluxio);
      return GetStatusFromAlluxioException(statusName, errorMsg);
    } else {
      return Status::OK();
    }
  }

  static std::string JstringToString(jstring jstr) {
    JNIEnv* env = GetEnv();
    char* rtn = NULL;
    jclass clsString = env->FindClass("java/lang/String");
    jstring stringCode = env->NewStringUTF("GB2312");
    jmethodID mid = env->GetMethodID(clsString, "getBytes",
                                     "(Ljava/lang/String;)[B");
    jbyteArray byteArray = (jbyteArray)env->CallObjectMethod(jstr, mid,
                                                             stringCode);
    jsize byteArrayLength = env->GetArrayLength(byteArray);
    jbyte* bArray = env->GetByteArrayElements(byteArray, JNI_FALSE);
    if (byteArrayLength > 0) {
      rtn = reinterpret_cast<char*>(malloc(byteArrayLength + 1));
      memcpy(rtn, bArray, byteArrayLength);
      rtn[byteArrayLength] = 0;
    }
    env->ReleaseByteArrayElements(byteArray, bArray, 0);
    std::string stemp(rtn);
    free(rtn);
    return stemp;
  }

  static void CacheClassName(jobject obj, const std::string& classname) {
    ClassCache::instance(JniHelper::GetEnv())->CacheClassName(obj, classname);
  }

  static void DeleteClassName(jobject obj) {
    ClassCache::instance(JniHelper::GetEnv())->deleteClassName(obj);
  }

  static jstring SringToJstring(JNIEnv *env, const char *pat);

 private:
  static JNIEnv* CacheEnv(JavaVM* jvm);
  static bool GetMethodInfo(JniMethodInfo* methodinfo, const char* className,
                            const char *methodName, const char* paramCode,
                            bool isStatic);
  static JavaVM* _psJavaVM;
  static jstring Convert(LocalRefMapType* localRefs, JniMethodInfo* t,
                         const char* x);
  static jstring Convert(LocalRefMapType* localRefs, JniMethodInfo* t,
                         const std::string& x);

  template <typename T>
  static T Convert(LocalRefMapType* localRefs, JniMethodInfo*, T x) {
    return x;
  }

  static std::string GetJniSignature() {
    return "";
  }

  static std::string GetJniSignature(int) {
    return "I";
  }

  static std::string GetJniSignature(bool) {
    return "Z";
  }

  static std::string GetJniSignature(char) {
    return "C";
  }

  static std::string GetJniSignature(int16_t) {
    return "S";
  }

  static std::string GetJniSignature(int64_t) {
    return "J";
  }

  static std::string GetJniSignature(float) {
    return "F";
  }

  static std::string GetJniSignature(double) {
    return "D";
  }

  static std::string GetJniSignature(const char*) {
    return "[B";
  }

  static std::string GetJniSignature(jbyteArray) {
    return "[B";
  }

  static std::string GetJniSignature(jobject obj) {
    return ClassCache::instance(JniHelper::GetEnv())->getClassName(obj);
  }

  static std::string GetJniSignature(const std::string&) {
    return "Ljava/lang/String;";
  }

  template <typename T>
  static std::string GetJniSignature(T x) {
    // This template should never be instantiated
    return "";
  }

  template <typename T, typename... Ts>
  static std::string GetJniSignature(T x, Ts... xs) {
    return GetJniSignature(x) + GetJniSignature(xs...);
  }

  static void ReportError(const std::string& className,
                          const std::string& methodName,
                          const std::string& signature);
};

}  // namespace jnihelper

#endif  // CPP_INCLUDE_JNIHELPER_H_
