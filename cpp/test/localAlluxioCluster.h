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

#ifndef CPP_TEST_LOCALALLUXIOCLUSTER_H_
#define CPP_TEST_LOCALALLUXIOCLUSTER_H_

#include "jniHelper.h"
#include "fileSystem.h"

using ::jnihelper::JniHelper;

namespace alluxio {

class LocalAlluxioCluster {
 public:
  LocalAlluxioCluster() {
    JniHelper::Start();
    miniCluster = JniHelper::CreateObjectMethod(
        "alluxio/master/LocalAlluxioCluster");
  }

  void start() {
    JniHelper::CallVoidMethod(miniCluster,
                              "alluxio/master/AbstractLocalAlluxioCluster",
                              "initConfiguration");
    JniHelper::CallVoidMethod(miniCluster,
                              "alluxio/master/AbstractLocalAlluxioCluster",
                              "setupTest");
    JniHelper::CallVoidMethod(miniCluster,
                              "alluxio/master/LocalAlluxioCluster",
                              "startMasters");
    JniHelper::CallVoidMethod(miniCluster,
                              "alluxio/master/AbstractLocalAlluxioCluster",
                              "startWorkers");
  }

  void getClient(FileSystem** fileSystem) {
    jobject jfileSystem = JniHelper::CallObjectMethod(miniCluster,
                                         "alluxio/master/LocalAlluxioCluster",
                                         "getClient",
                                         "alluxio/client/file/FileSystem");
    *fileSystem = new FileSystem(jfileSystem);
  }

  ~LocalAlluxioCluster() {
    JniHelper::CallVoidMethod(miniCluster,
                              "alluxio/master/AbstractLocalAlluxioCluster",
                              "stop");
    JniHelper::DeleteObjectRef(miniCluster);
  }

  const jobject& getObj() const {
    return miniCluster;
  }

 protected:
  jobject miniCluster;
};

}  // namespace alluxio

#endif  // CPP_TEST_LOCALALLUXIOCLUSTER_H_
