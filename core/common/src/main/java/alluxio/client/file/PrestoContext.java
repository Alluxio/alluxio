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

package alluxio.client.file;

import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import alluxio.wire.FileInfo;

import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Context when running in Presto.
 */
public class PrestoContext {
  private CacheQuota mCacheQuota = CacheQuota.UNLIMITED;
  private CacheScope mCacheScope = CacheScope.GLOBAL;

  /**
   * Constructs PrestoContext.
   */
  public PrestoContext() {
  }

  /**
   * @return the cache quota
   */
  public CacheQuota getCacheQuota() {
    return mCacheQuota;
  }

  /**
   * @return the cache scope
   */
  public CacheScope getCacheScope() {
    return mCacheScope;
  }

  /**
   * @param cacheQuota the cache quota
   * @return the updated {@link FileInfo}
   */
  public PrestoContext setCacheQuota(CacheQuota cacheQuota) {
    mCacheQuota = cacheQuota;
    return this;
  }

  /**
   * @param cacheScope the cache quota
   * @return the updated {@link FileInfo}
   */
  public PrestoContext setCacheScope(CacheScope cacheScope) {
    mCacheScope = cacheScope;
    return this;
  }

  /**
   * Increments the counter {@code name} by {@code value}.
   * <p>
   * Default implementation does nothing. Subclass can implement its own tracking mechanism.
   *
   * @param name name of the counter
   * @param value value of the counter
   */
  public void incrementCounter(String name, long value) {
    // Default implementation does nothing
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PrestoContext that = (PrestoContext) o;
    return Objects.equals(mCacheQuota, that.mCacheQuota) && Objects
        .equals(mCacheScope, that.mCacheScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mCacheQuota, mCacheScope);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("cacheQuota", mCacheQuota)
        .add("cacheScope", mCacheScope)
        .toString();
  }
}
