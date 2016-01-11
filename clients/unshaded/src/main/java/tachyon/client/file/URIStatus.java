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

import com.google.common.base.Preconditions;

import tachyon.annotation.PublicApi;
import tachyon.thrift.FileInfo;

/**
 * Wrapper around {@link FileInfo}. Represents the metadata about a file or directory in Tachyon.
 * This is a snapshot of information about the file or directory and not all attributes are
 * guaranteed to be up to date. Attributes documented as immutable will always be accurate, and
 * attributes documented as mutable may be out of date.
 */
@PublicApi
public class URIStatus {
  private final FileInfo mInfo;

  /**
   * Constructs an instance of this class from a {@link FileInfo}
   * @param info an object containing the information about a particular uri
   */
  public URIStatus(FileInfo info) {
    mInfo = Preconditions.checkNotNull(info, "Cannot create a URIStatus from a null FileInfo");
  }

  /**
   * @return a list of block ids belonging to the file, empty for directories, immutable
   */
  public List<Long> getBlockIds() {
    return mInfo.getBlockIds();
  }

  /**
   * @return the default block size for this file, 0 for directories, immutable
   */
  public long getBlockSizeBytes() {
    return mInfo.getBlockSizeBytes();
  }

  /**
   * @return the epoch time the entity referenced by this uri was created, immutable
   */
  public long getCreationTimeMs() {
    return mInfo.getCreationTimeMs();
  }

  /**
   * @return the unique identifier of the entity referenced by this uri used by Tachyon servers,
   *         immutable
   */
  public long getFileId() {
    return mInfo.getFileId();
  }

  /**
   * @return the group that owns the entity referenced by this uri, mutable
   */
  public String getGroupName() {
    return mInfo.getGroupName();
  }

  /**
   * @deprecated This method is temporary
   *
   * @return the backing {@link FileInfo} object
   */
  // TODO(calvin): Remove this method
  @Deprecated
  public FileInfo getInfo() {
    return mInfo;
  }

  /**
   * @return the percentage of blocks in Tachyon memory tier storage, mutable
   */
  public int getInMemoryPercentage() {
    return mInfo.getInMemoryPercentage();
  }

  /**
   * @return the epoch time the entity referenced by this uri was last modified, mutable
   */
  public long getLastModificationTimeMs() {
    return mInfo.getLastModificationTimeMs();
  }

  /**
   * @return the length in bytes of the file, 0 for directories, mutable
   */
  public long getLength() {
    return mInfo.getLength();
  }

  /**
   * For example for the uri: tachyon://host:1000/foo/bar/baz, baz is the name
   *
   * @return the last path component of the entity referenced by this uri, mutable
   */
  public String getName() {
    return mInfo.getName();
  }

  /**
   * For example, for the uri: tachyon://host:1000/foo/bar/baz, the path is /foo/bar/baz
   *
   * @return the entire path component of the entity referenced by this uri, mutable
   */
  public String getPath() {
    return mInfo.getPath();
  }

  /**
   * @return the int representation of the ACL permissions of the entity referenced by this uri,
   *         mutable
   */
  public int getPermission() {
    return mInfo.getPermission();
  }

  /**
   * @return the string representation of the persistence status, mutable
   */
  // TODO(calvin): Consider returning the enum if it is moved to common
  public String getPersistenceState() {
    return mInfo.getPersistenceState();
  }

  /**
   * @return the time-to-live in milliseconds since the creation time of the entity referenced by
   *         this uri, mutable
   */
  public long getTtl() {
    return mInfo.getTtl();
  }

  /**
   * @return the uri of the under storage location of the entity referenced by this uri, mutable
   */
  public String getUfsPath() {
    return mInfo.getUfsPath();
  }

  /**
   * @return the user which owns the entity referenced by this uri, mutable
   */
  public String getUserName() {
    return mInfo.getUserName();
  }

  /**
   * @return whether the entity referenced by this uri can be stored in Tachyon space, mutable
   */
  public boolean isCacheable() {
    return mInfo.isIsCacheable();
  }

  /**
   * @return whether the entity referenced by this uri has been marked as completed, immutable
   */
  public boolean isCompleted() {
    return mInfo.isIsCompleted();
  }

  /**
   * @return whether the entity referenced by this uri is a directory, immutable
   */
  // TODO(calvin): Consider consolidating the terms directory and folder
  public boolean isFolder() {
    return mInfo.isIsFolder();
  }

  /**
   * @return whether the entity referenced by this uri is persisted to an underlying storage,
   *         mutable
   */
  public boolean isPersisted() {
    return mInfo.isIsPersisted();
  }

  /**
   * @return whether the entity referenced by this uri is pinned, mutable
   */
  public boolean isPinned() {
    return mInfo.isIsPinned();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof URIStatus)) {
      return false;
    }
    URIStatus that = (URIStatus) o;
    return mInfo.equals(that.mInfo);
  }

  @Override
  public int hashCode() {
    return mInfo.hashCode();
  }

  @Override
  public String toString() {
    return mInfo.toString();
  }
}
