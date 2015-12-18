/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file;

import java.util.List;

import tachyon.thrift.FileInfo;

/**
 * Wrapper around {@link FileInfo}. Represents the metadata about a file or folder in Tachyon.
 */
public class PathStatus {
  private final FileInfo mInfo;

  public PathStatus(FileInfo info) {
    mInfo = info;
  }

  public List<Long> getBlockIds() {
    return mInfo.getBlockIds();
  }

  public long getBlockSizeBytes() {
    return mInfo.getBlockSizeBytes();
  }

  public long getCreationTimeMs() {
    return mInfo.getCreationTimeMs();
  }

  public long getFileId() {
    return mInfo.getFileId();
  }

  public String getGroupName() {
    return mInfo.getGroupName();
  }

  public int getInMemoryPercentage() {
    return mInfo.getInMemoryPercentage();
  }

  public long getLastModificationTimeMs() {
    return mInfo.getLastModificationTimeMs();
  }

  public long getLength() {
    return mInfo.getLength();
  }

  public String getName() {
    return mInfo.getName();
  }

  public String getPath() {
    return mInfo.getPath();
  }

  public int getPermission() {
    return mInfo.getPermission();
  }

  public long getTtl() {
    return mInfo.getTtl();
  }

  public String getUfsPath() {
    return mInfo.getUfsPath();
  }

  public String getUsername() {
    return mInfo.getUserName();
  }
}
