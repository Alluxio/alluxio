/*
 * The Alluxio Open Foundation licenses this work under the Apache License,
 * version 2.0 (the "License"). You may not use this work except in compliance
 * with the License, which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied, as more fully set forth
 * in the License.
 *
 * See the NOTICE file distributed with this work for information regarding
 * copyright ownership.
 */


#ifndef WIRE_H
#define WIRE_H

namespace alluxio {

class MountPointInfo {
  public:
    MountPointInfo(jobject mountPointInfo) {
      jMountPointInfo = mountPointInfo;
    }

    MountPointInfo(const MountPointInfo& s) {
      jMountPointInfo=  JniHelper::GetEnv()->NewGlobalRef(s.jMountPointInfo);
    }

    void operator = (const MountPointInfo& s) {
      jMountPointInfo=  JniHelper::GetEnv()->NewGlobalRef(s.jMountPointInfo);
    }

    ~MountPointInfo() {
      JniHelper::DeleteObjectRef(jMountPointInfo);
    }

    std::string ToString() {
        try {
          return JniHelper::CallStringMethod(jMountPointInfo, "alluxio/wire/MountPointInfo", "toString");
        } catch (std::string e ) {
          return "";
        }
    }
   private:
    jobject jMountPointInfo;
};

class URIStatus {
  public:
    URIStatus(jobject URIStatus) {
        jURIStatus = URIStatus;
    }

    URIStatus(const URIStatus& s) {
      jURIStatus=  JniHelper::GetEnv()->NewGlobalRef(s.jURIStatus);
    }

    void operator = (const URIStatus& s) {
      jURIStatus=  JniHelper::GetEnv()->NewGlobalRef(s.jURIStatus);
    }

    ~URIStatus() {
        JniHelper::DeleteObjectRef(jURIStatus);
    }

    std::string ToString() {
        try {
          return JniHelper::CallStringMethod(jURIStatus, "alluxio/client/file/URIStatus", "toString");
        } catch (std::string e ) {
          return "";
        }
    }

  private:
    jobject jURIStatus;
};

}

#endif // WIRE_H
