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

import alluxio.client.hive.HiveCacheContext;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;

import com.google.common.base.MoreObjects;

import java.util.Objects;

import javax.annotation.Nullable;

/**
 * Cache related context.
 */
public class CacheContext {
  /** Used in Prestodb to indicate the cache quota for a file. */
  private CacheQuota mCacheQuota = CacheQuota.UNLIMITED;

  /** Used in Prestodb to indicate the cache scope. */
  private CacheScope mCacheScope = CacheScope.GLOBAL;

  /** Used in Prestodb to indicate the hiveContext for a file. */
  private HiveCacheContext mHiveCacheContext = null;

  /**
   * Used in Prestodb to uniquely identify a file in Alluxio local cache.
   * Note that, though the filePath can be a unique identifier, it can be a long string
   * hence using md5 hash of the file path as the identifier in the cache.
   * We don't set Alluxio fileId because in local cache files do not have Alluxio fileId assigned.
   */
  private String mCacheIdentifier = null;

  /**
   * @return the default CacheContext
   */
  public static CacheContext defaults() {
    return new CacheContext();
  }

  /**
   * Expected to be inherited in PrestoDB or other local cache caller.
   * Subclasses could override the callback methods such as incrementCounter
   */
  protected CacheContext() {}

  /**
   * Returns an string as a hint from computation to indicate the file.
   * Note that, this can be independent and different from Alluxio File ID stored in
   * URIStatus.mInfo, which is assigned and maintained by Alluxio master.
   * In cases like using Alluxio local cache, such Alluxio File ID may not be available.
   *
   * @return the unique string identifier of the entity to cache
   */
  @Nullable
  public String getCacheIdentifier() {
    return mCacheIdentifier;
  }

  /**
   * @return the hive cache context
   */
  @Nullable
  public HiveCacheContext getHiveCacheContext() {
    return mHiveCacheContext;
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
   * @param identifier the id to use
   * @return the updated {@code CacheContext}
   */
  public CacheContext setCacheIdentifier(String identifier) {
    mCacheIdentifier = identifier;
    return this;
  }

  /**
   * @param cacheQuota the cache quota
   * @return the updated {@code CacheContext}
   */
  public CacheContext setCacheQuota(CacheQuota cacheQuota) {
    mCacheQuota = cacheQuota;
    return this;
  }

  /**
   * @param cacheScope the cache quota
   * @return the updated {@code CacheContext}
   */
  public CacheContext setCacheScope(CacheScope cacheScope) {
    mCacheScope = cacheScope;
    return this;
  }

  /**
   * @param hiveCacheContext the hive cache context
   * @return the updated {@code CacheContext}
   */
  public CacheContext setHiveCacheContext(HiveCacheContext hiveCacheContext) {
    mHiveCacheContext = hiveCacheContext;
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
    CacheContext that = (CacheContext) o;
    return Objects.equals(mCacheIdentifier, that.mCacheIdentifier)
        && Objects.equals(mHiveCacheContext, that.mHiveCacheContext)
        && Objects.equals(mCacheQuota, that.mCacheQuota)
        && Objects.equals(mCacheScope, that.mCacheScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mCacheQuota, mCacheScope, mCacheIdentifier, mHiveCacheContext);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("cacheIdentifier", mCacheIdentifier)
        .add("cacheQuota", mCacheQuota)
        .add("cacheScope", mCacheScope)
        .add("hiveCacheContext", mHiveCacheContext)
        .toString();
  }
}
