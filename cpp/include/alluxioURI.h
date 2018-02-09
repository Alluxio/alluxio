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

#ifndef CPP_INCLUDE_ALLUXIOURI_H_
#define CPP_INCLUDE_ALLUXIOURI_H_

#include "jniHelper.h"

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <string>

using ::jnihelper::JniHelper;

namespace alluxio {
class AlluxioURI {
 public:
  explicit AlluxioURI(const std::string& path) {
    uri = JniHelper::CreateObjectMethod("alluxio/AlluxioURI", path);
  }

  explicit AlluxioURI(jobject localObj) {
    uri = localObj;
  }

  ~AlluxioURI() {
    JniHelper::DeleteObjectRef(uri);
  }

  const jobject& getObj() const {
    return uri;
  }

 protected:
  jobject uri;
};

}  // namespace alluxio

#endif  // CPP_INCLUDE_ALLUXIOURI_H_
