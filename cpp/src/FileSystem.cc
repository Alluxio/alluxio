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

#include "FileSystem.h"

FileSystem::FileSystem() {
  JniHelper::Start();
  FileSystem::filesystem = JniHelper::CallStaticObjectMethod(
      "alluxio/client/file/FileSystem$Factory", "get",
      "alluxio/client/file/FileSystem");
}

void FileSystem::closeFileSystem() {
  JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
  JniHelper::DeleteObjectRef(FileSystem::filesystem);
}

FileSystem::~FileSystem() {
  FileSystem::closeFileSystem();
}

Status FileSystem::CreateDirectory(const AlluxioURI& path) {
  return FileSystem::callJniBydefaultOption(path, "createDirectory");
}

Status FileSystem::CreateDirectory(const AlluxioURI& path,
                                   const CreateDirectoryOptions& options) {
  return FileSystem::callJniByOption(path, "createDirectory",
                                     options.getOptions());
}

Status FileSystem::CreateFile(const AlluxioURI& path,
                              FileOutStream** outStream) {
  try {
    jobject fileOutStream = JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem", "createFile",
        "alluxio/client/file/FileOutStream",  path.getObj());
    Status status = JniHelper::AlluxioExceptionCheck();
    if (status.ok()) {
      *outStream = new FileOutStream(fileOutStream);
     }
    return status;
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::CreateFile(const AlluxioURI& path,
                              const CreateFileOptions& options,
                              FileOutStream** outStream) {
  try {
    jobject fileOutStream = JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem", "createFile",
		"alluxio/client/file/FileOutStream", path.getObj(),
		options.getOptions());
    Status status = JniHelper::AlluxioExceptionCheck();
    if (status.ok()) {
      *outStream = new FileOutStream(fileOutStream);
    }
    return status;
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::DeletePath(const AlluxioURI& path) {
  return callJniBydefaultOption(path, "delete");
}

Status FileSystem::DeletePath(const AlluxioURI& path,
                              const DeleteOptions& options) {
  return callJniByOption(path, "delete", options.getOptions());
}

Status FileSystem::Exists(const AlluxioURI& path, bool* result) {
  try {
    bool res = JniHelper::CallBooleanMethod(FileSystem::filesystem,
                                            "alluxio/client/file/FileSystem",
                                            "exists", path.getObj());
    result = &res;
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::Exists(const AlluxioURI& path, const ExistsOptions& options,
                          bool* result) {
  bool res;
  try {
    res = JniHelper::CallBooleanMethod(FileSystem::filesystem,
                                       "alluxio/client/file/FileSystem",
                                       "exists", path.getObj(),
                                       options.getOptions());
    result = &res;
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::Free(const AlluxioURI& path) {
  return callJniBydefaultOption(path, "free");
}

Status FileSystem::Free(const AlluxioURI& path, const FreeOptions& options) {
  return callJniByOption(path, "free", options.getOptions());
}

Status FileSystem::GetStatus(const AlluxioURI& path, URIStatus** result) {
  try {
    jobject uriStatus = JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem","getStatus",
        "alluxio/client/file/URIStatus", path.getObj());
    *result = new URIStatus(uriStatus);
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::GetStatus(const AlluxioURI& path,
                             const GetStatusOptions& options,
                             URIStatus** result) {
  try {
    jobject uriStatus = JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem","getStatus",
        "alluxio/client/file/URIStatus", path.getObj(), options.getOptions());
    *result = new URIStatus(uriStatus);
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::ListStatus(const AlluxioURI& path,
                              std::vector<URIStatus>* result) {
  JniHelper::LocalRefMapType localRefs;
  try {
    jobject uriStatusList =  JniHelper::CallObjectMethod(
        FileSystem::filesystem, "alluxio/client/file/FileSystem", "listStatus",
        "java/util/List", path.getObj());
    localRefs[JniHelper::GetEnv()].push_back(uriStatusList);
    int listSize = JniHelper::CallIntMethod(uriStatusList, "java/util/List",
                                            "size");

    for (int i = 0; i < listSize; i ++) {
      jobject alluxioUriStatus = JniHelper::CallObjectMethod(
          uriStatusList, "java/util/List", "get", "java/lang/Object", i);
      result->push_back(URIStatus(alluxioUriStatus));
    }
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return Status::jniError(e);
  }
}

Status FileSystem::ListStatus(const AlluxioURI& path,
                              const ListStatusOptions& options,
                              std::vector<URIStatus>* result) {
  try {
    jobject uriStatusList =  JniHelper::CallObjectMethod(
        FileSystem::filesystem, "alluxio/client/file/FileSystem", "listStatus",
        "java/util/List", path.getObj(), options.getOptions());
    localRefs[JniHelper::GetEnv()].push_back(uriStatusList);
    int listSize = JniHelper::CallIntMethod(uriStatusList, "java/util/List",
                                            "size");
    for (int i = 0; i < listSize; i ++) {
      jobject alluxioUriStatus = JniHelper::CallObjectMethod(
          uriStatusList, "java/util/List", "get", "java/lang/Object", i);
      result->push_back(URIStatus(alluxioUriStatus));
    }
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return Status::jniError(e);
  }
}

Status FileSystem::Mount(const AlluxioURI& alluxioPath,
                         const AlluxioURI& ufsPath) {
  return callJniBydefaultOption(alluxioPath, ufsPath, "mount");
}

Status FileSystem::Mount(const AlluxioURI& alluxioPath,
                         const AlluxioURI& ufsPath,
                         const MountOptions& options) {
  return callJniByOption(alluxioPath, ufsPath, "mount", options.getOptions());
}

Status FileSystem::GetMountTable(
	  std::map<std::string, MountPointInfo>* result) {
  try {
    jobject jMountTable =  JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem", "getMountTable", "java/util/Map");
    localRefs[JniHelper::GetEnv()].push_back(jMountTable);

    int mapSize = JniHelper::CallIntMethod(jMountTable, "java/util/Map",
                                           "size");
    jobject keySet =  JniHelper::CallObjectMethod(jMountTable, "java/util/Map",
                                                  "keySet", "java/util/Set");
    localRefs[JniHelper::GetEnv()].push_back(keySet);
    jobject keyArray = JniHelper::CallObjectMethod(keySet, "java/util/Set",
                                                   "toArray",
                                                   "[Ljava/lang/Object");
    localRefs[JniHelper::GetEnv()].push_back(keyArray);
    for(int i = 0; i < mapSize; i ++) {
      jobject keyItem = JniHelper::GetEnv()->
          GetObjectArrayElement((jobjectArray) keyArray, i);
      JniHelper::CacheClassName(keyItem, "java/lang/Object");
      std::string key = JniHelper::JstringToString((jstring)keyItem);
      jobject valueItem = JniHelper::CallObjectMethod(jMountTable,
                                                      "java/util/Map", "get",
                                                      "java/lang/Object",
                                                      (jobject)keyItem);
      result->insert(std::make_pair(key, MountPointInfo(valueItem)));
	  JniHelper::DeleteObjectRef(keyItem);
    }
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return JniHelper::AlluxioExceptionCheck();
  } catch(std::string e) {
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    return Status::jniError(e);
  }
}

Status FileSystem::OpenFile(const AlluxioURI& path, FileInStream** inStream) {
  jobject fileInStream;
  try {
    fileInStream = JniHelper::CallObjectMethod(FileSystem::filesystem,
        "alluxio/client/file/FileSystem","openFile",
        "alluxio/client/file/FileInStream", path.getObj());
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    Status stus = JniHelper::AlluxioExceptionCheck();
    if (stus.ok()) {
      *inStream = new FileInStream(fileInStream);
    }
    return stus;
  } catch (std::string e) {
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
  	return Status::jniError(e);
  }
}

Status FileSystem::OpenFile(const AlluxioURI& path,
                            const OpenFileOptions& options,
                            FileInStream** inStream) {
  jobject fileInStream;
  try {
    fileInStream = JniHelper::CallObjectMethod(
        FileSystem::filesystem, "alluxio/client/file/FileSystem","openFile",
        "alluxio/client/file/FileInStream", path.getObj(), options.getOptions());
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
    Status stus = JniHelper::AlluxioExceptionCheck();
    if (stus.ok()) {
      *inStream = new FileInStream(fileInStream);
    }
    return stus;
  } catch (std::string e) {
    JniHelper::DeleteLocalRefs(JniHelper::GetEnv(), localRefs);
  	return Status::jniError(e);
  }
}

Status FileSystem::Rename(const AlluxioURI& src, const AlluxioURI& dst) {
  return callJniBydefaultOption(src, dst, "rename");
}

Status FileSystem::Rename(const AlluxioURI& src, const AlluxioURI& dst,
                          const RenameOptions& options) {
  return callJniByOption(src, dst, "rename", options.getOptions());
}

Status FileSystem::SetAttribute(const AlluxioURI& path) {
  return callJniBydefaultOption(path, "setAttribute");
}

Status FileSystem::SetAttribute(const AlluxioURI& path,
                                const SetAttributeOptions& options) {
  return callJniByOption(path, "setAttribute", options.getOptions());
}

Status FileSystem::Unmount(const AlluxioURI& path) {
  return callJniBydefaultOption(path, "unmount");
}

Status FileSystem::Unmount(const AlluxioURI& path,
                           const UnmountOptions& options) {
  return callJniByOption(path, "unmount", options.getOptions());
}

Status FileSystem::callJniBydefaultOption(const AlluxioURI& path,
                                          const std::string& methodName) {
  try {    
    JniHelper::CallVoidMethod(FileSystem::filesystem,
                              "alluxio/client/file/FileSystem", methodName,
                              path.getObj());
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::callJniBydefaultOption(const AlluxioURI& src,
                                          const AlluxioURI& dst,
                                          const std::string& methodName) {
  try {
    JniHelper::CallVoidMethod(FileSystem::filesystem,
                              "alluxio/client/file/FileSystem", methodName,
                              src.getObj(), dst.getObj());
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::callJniByOption(const AlluxioURI& path,
                                   const std::string& methodName,
                                   const jobject option) {
  try {
    JniHelper::CallVoidMethod(FileSystem::filesystem,
                              "alluxio/client/file/FileSystem", methodName,
                              path.getObj(), option);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}

Status FileSystem::callJniByOption(const AlluxioURI& src,
                                   const AlluxioURI& dst,
                                   const std::string& methodName,
                                   const jobject option) {
  try {
    JniHelper::CallVoidMethod(FileSystem::filesystem,
                              "alluxio/client/file/FileSystem", methodName,
                              src.getObj(), dst.getObj(), option);
    return JniHelper::AlluxioExceptionCheck();
  } catch (std::string e) {
    return Status::jniError(e);
  }
}
