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

import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import java.util.List;

/**
 * Options for initiating local cache manager.
 */
public class CacheManagerOptions {
  private boolean mAsyncRestoreEnabled;
  private boolean mAsyncWriteEnabled;
  private int mAsyncWriteThreads;
  private CacheEvictorOptions mCacheEvictorOptions;
  private int mMaxEvictionRetries;
  private long mPageSize;
  private List<PageStoreOptions> mPageStoreOptions;
  private boolean mQuotaEnabled;
  private boolean mTtlEnabled;
  private long mTtlCheckIntervalSeconds;
  private long mTtlThresholdSeconds;

  /**
   * @param conf
   * @return instance of CacheManagerOptions
   */
  public static CacheManagerOptions create(AlluxioConfiguration conf) {
    CacheEvictorOptions cacheEvictorOptions = new CacheEvictorOptions()
        .setEvictorClass(conf.getClass(PropertyKey.USER_CLIENT_CACHE_EVICTOR_CLASS))
        .setIsNondeterministic(
            conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_EVICTOR_NONDETERMINISTIC_ENABLED))
        .setLFULogBase(conf.getDouble(PropertyKey.USER_CLIENT_CACHE_EVICTOR_LFU_LOGBASE));
    CacheManagerOptions options = new CacheManagerOptions()
        .setAsyncRestoreEnabled(
            conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED))
        .setAsyncWriteThreads(conf.getInt(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_THREADS))
        .setIsAsyncWriteEnabled(
            conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED))
        .setMaxEvictionRetries(conf.getInt(PropertyKey.USER_CLIENT_CACHE_EVICTION_RETRIES))
        .setPageSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE))
        .setQuotaEnabled(conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED))
        .setTtlEnabled(conf.getBoolean(PropertyKey.USER_CLIENT_CACHE_TTL_ENABLED))
        .setTtlCheckIntervalSeconds(
            conf.getLong(PropertyKey.USER_CLIENT_CACHE_TTL_CHECK_INTERVAL_SECONDS))
        .setTtlThresholdSeconds(conf.getLong(PropertyKey.USER_CLIENT_CACHE_TTL_THRESHOLD_SECONDS))
        .setCacheEvictorOptions(cacheEvictorOptions)
        .setPageStoreOptions(PageStoreOptions.create(conf));
    return options;
  }

  /**
   * @param conf
   * @return instance of CacheManagerOptions
   */
  public static CacheManagerOptions createForWorker(AlluxioConfiguration conf) {
    CacheEvictorOptions cacheEvictorOptions = new CacheEvictorOptions()
        .setEvictorClass(conf.getClass(PropertyKey.WORKER_PAGE_STORE_EVICTOR_CLASS))
        .setIsNondeterministic(
            conf.getBoolean(PropertyKey.WORKER_PAGE_STORE_EVICTOR_NONDETERMINISTIC_ENABLED))
        .setLFULogBase(conf.getDouble(PropertyKey.WORKER_PAGE_STORE_EVICTOR_LFU_LOGBASE));
    CacheManagerOptions options = new CacheManagerOptions()
        .setAsyncRestoreEnabled(
            conf.getBoolean(PropertyKey.WORKER_PAGE_STORE_ASYNC_RESTORE_ENABLED))
        .setAsyncWriteThreads(conf.getInt(PropertyKey.WORKER_PAGE_STORE_ASYNC_WRITE_THREADS))
        .setIsAsyncWriteEnabled(
            conf.getBoolean(PropertyKey.WORKER_PAGE_STORE_ASYNC_WRITE_ENABLED))
        .setMaxEvictionRetries(conf.getInt(PropertyKey.WORKER_PAGE_STORE_EVICTION_RETRIES))
        .setPageSize(conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE))
        .setQuotaEnabled(conf.getBoolean(PropertyKey.WORKER_PAGE_STORE_QUOTA_ENABLED))
        .setCacheEvictorOptions(cacheEvictorOptions)
        .setPageStoreOptions(PageStoreOptions.createForWorkerPageStore(conf));
    return options;
  }

  /**
   * Constructor.
   */
  public CacheManagerOptions() {
  }

  /**
   * @return if async restore is enabled
   */
  public boolean isAsyncRestoreEnabled() {
    return mAsyncRestoreEnabled;
  }

  /**
   * @return if async write is enabled
   */
  public boolean isAsyncWriteEnabled() {
    return mAsyncWriteEnabled;
  }

  /**
   * @return number of thread for async write
   */
  public int getAsyncWriteThreads() {
    return mAsyncWriteThreads;
  }

  /**
   * @return if quota is enabled
   */
  public boolean isQuotaEnabled() {
    return mQuotaEnabled;
  }

  /**
   * @return if cache ttl is enabled
   */
  public boolean isTtlEnabled() {
    return mTtlEnabled;
  }

  /**
   * @return the check interval of ttl
   */
  public long getTtlCheckIntervalSeconds() {
    return mTtlCheckIntervalSeconds;
  }

  /**
   *
   * @return the time threshold of cache ttl
   */
  public long getTtlThresholdSeconds() {
    return mTtlThresholdSeconds;
  }

  /**
   * @return max eviction retires
   */
  public int getMaxEvictionRetries() {
    return mMaxEvictionRetries;
  }

  /**
   * @return the page size
   */
  public long getPageSize() {
    return mPageSize;
  }

  /**
   * @return the list of PageStoreOptions
   */
  public List<PageStoreOptions> getPageStoreOptions() {
    return mPageStoreOptions;
  }

  /**
   * @return the options of cache evictor
   */
  public CacheEvictorOptions getCacheEvictorOptions() {
    return mCacheEvictorOptions;
  }

  /**
   * @param isAsyncRestoreEnabled
   * @return the updated options
   */
  public CacheManagerOptions setAsyncRestoreEnabled(boolean isAsyncRestoreEnabled) {
    mAsyncRestoreEnabled = isAsyncRestoreEnabled;
    return this;
  }

  /**
   * @param isAsyncWriteEnabled
   * @return the updated options
   */
  public CacheManagerOptions setIsAsyncWriteEnabled(boolean isAsyncWriteEnabled) {
    mAsyncWriteEnabled = isAsyncWriteEnabled;
    return this;
  }

  /**
   * @param asyncWriteThreads
   * @return the updated options
   */
  public CacheManagerOptions setAsyncWriteThreads(int asyncWriteThreads) {
    mAsyncWriteThreads = asyncWriteThreads;
    return this;
  }

  /**
   * @param cacheEvictorOptions
   * @return the updated options
   */
  public CacheManagerOptions setCacheEvictorOptions(CacheEvictorOptions cacheEvictorOptions) {
    mCacheEvictorOptions = cacheEvictorOptions;
    return this;
  }

  /**
   * @param maxEvictionRetries
   * @return the updated options
   */
  public CacheManagerOptions setMaxEvictionRetries(int maxEvictionRetries) {
    mMaxEvictionRetries = maxEvictionRetries;
    return this;
  }

  /**
   * @param pageSize
   * @return the updated options
   */
  public CacheManagerOptions setPageSize(long pageSize) {
    mPageSize = pageSize;
    return this;
  }

  /**
   * @param isQuotaEnabled
   * @return the updated options
   */
  public CacheManagerOptions setQuotaEnabled(boolean isQuotaEnabled) {
    mQuotaEnabled = isQuotaEnabled;
    return this;
  }

  /**
   * @param pageStoreOptions
   * @return the updated options
   */
  public CacheManagerOptions setPageStoreOptions(
      List<PageStoreOptions> pageStoreOptions) {
    mPageStoreOptions = pageStoreOptions;
    return this;
  }

  /**
   * @param isTtlEnabled
   * @return the updated options
   */
  public CacheManagerOptions setTtlEnabled(boolean isTtlEnabled) {
    mTtlEnabled = isTtlEnabled;
    return this;
  }

  /**
   * @param checkIntervalSeconds
   * @return the updated options
   */
  public CacheManagerOptions setTtlCheckIntervalSeconds(long checkIntervalSeconds) {
    mTtlCheckIntervalSeconds = checkIntervalSeconds;
    return this;
  }

  /**
   * @param thresholdSeconds
   * @return the updated options
   */
  public CacheManagerOptions setTtlThresholdSeconds(long thresholdSeconds) {
    mTtlThresholdSeconds = thresholdSeconds;
    return this;
  }
}
