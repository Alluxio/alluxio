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

package alluxio.client.file.cache;

import alluxio.client.quota.CacheScope;

import com.google.common.base.MoreObjects;

import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class identifies the information of a cached file.
 */
@ThreadSafe
public class FileInfo {

  private final CacheScope mCacheScope;
  private final long mLastModificationTimeMs;

  /**
   * @param cacheScope Cache scope
   * @param lastModificationTimeMs last modified time
   */
  public FileInfo(CacheScope cacheScope, long lastModificationTimeMs) {
    mCacheScope = cacheScope;
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  /**
   * @return scope of this file
   */
  public CacheScope getScope() {
    return mCacheScope;
  }

  /**
   * @return the last modification time in ms of this file
   */
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileInfo fileInfo = (FileInfo) o;
    return mLastModificationTimeMs == fileInfo.mLastModificationTimeMs && Objects
        .equals(mCacheScope, fileInfo.mCacheScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mCacheScope, mLastModificationTimeMs);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("mCacheScope", mCacheScope)
        .add("mLastModificationTimeMs", mLastModificationTimeMs)
        .toString();
  }
}
