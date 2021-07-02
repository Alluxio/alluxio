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

package alluxio.client.file.cache.store;

import alluxio.client.file.cache.PageStore;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import java.nio.file.Path;

/**
 * Options used to instantiate a {@link alluxio.client.file.cache.PageStore}.
 */
public abstract class PageStoreOptions {

  /**
   * @param conf configuration
   * @return a new instance of {@link PageStoreOptions}
   */
  public static PageStoreOptions create(AlluxioConfiguration conf) {
    PageStoreOptions options;
    PageStoreType storeType = conf.getEnum(
        PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.class);
    switch (storeType) {
      case LOCAL:
        options = new LocalPageStoreOptions()
            .setFileBuckets(conf.getInt(PropertyKey.USER_CLIENT_CACHE_LOCAL_STORE_FILE_BUCKETS));
        break;
      case ROCKS:
        options = new RocksPageStoreOptions();
        break;
      default:
        throw new IllegalArgumentException(String.format("Unrecognized store type %s",
            storeType.name()));
    }
    Path rootDir = PageStore.getStorePath(storeType, conf.get(PropertyKey.USER_CLIENT_CACHE_DIR));
    options.setRootDir(rootDir.toString())
        .setPageSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE))
        .setCacheSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_SIZE))
        .setAlluxioVersion(conf.get(PropertyKey.VERSION))
        .setTimeoutDuration(conf.getMs(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION))
        .setTimeoutThreads(conf.getInt(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_THREADS));
    if (conf.isSet(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD)) {
      options.setOverheadRatio(conf.getDouble(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD));
    }
    return options;
  }

  /**
   * @return the type corresponding to the page store
   */
  public abstract PageStoreType getType();

  /**
   *
   * @param <T> The type corresponding to the underlying options
   * @return the options casted to the required type
   */
  public <T> T toOptions() {
    return (T) this;
  }

  /**
   * Root directory where the data is stored.
   */
  protected String mRootDir;

  /**
   * Page size for the data.
   */
  protected long mPageSize;

  /**
   * Cache size for the data.
   */
  protected long mCacheSize;

  /**
   * Alluxio client version.
   */
  protected String mAlluxioVersion;

  /**
   * Timeout duration for page store operations in ms.
   */
  protected long mTimeoutDuration;

  /**
   * Number of threads for page store operations.
   */
  protected int mTimeoutThreads;

  /**
   * A fraction value representing the storage overhead.
   * i.e., with 1GB allocated cache space, and 10% storage overhead we
   * expect no more than 1024MB / (1 + 10%) user data to store
   */
  protected double mOverheadRatio;

  /**
   * @param rootDir the root directory where pages are stored
   * @return the updated options
   */
  public PageStoreOptions setRootDir(String rootDir) {
    mRootDir = rootDir;
    return this;
  }

  /**
   * @return the root directory where pages are stored
   */
  public String getRootDir() {
    return mRootDir;
  }

  /**
   * @return the size of the page in bytes
   */
  public long getPageSize() {
    return mPageSize;
  }

  /**
   * @param pageSize the size of the page in bytes
   * @return the updated options
   */
  public PageStoreOptions setPageSize(long pageSize) {
    mPageSize = pageSize;
    return this;
  }

  /**
   * @return the size of the cache in bytes
   */
  public long getCacheSize() {
    return mCacheSize;
  }

  /**
   * @param cacheSize the size of the cache in bytes
   * @return the updated options
   */
  public PageStoreOptions setCacheSize(long cacheSize) {
    mCacheSize = cacheSize;
    return this;
  }

  /**
   * @return the Alluxio client version
   */
  public String getAlluxioVersion() {
    return mAlluxioVersion;
  }

  /**
   * @param alluxioVersion Alluxio client version
   * @return the updated options
   */
  public PageStoreOptions setAlluxioVersion(String alluxioVersion) {
    mAlluxioVersion = alluxioVersion;
    return this;
  }

  /**
   * @return timeout duration for page store operations in ms
   */
  public long getTimeoutDuration() {
    return mTimeoutDuration;
  }

  /**
   * @param timeout timeout duration for page store operations in ms
   * @return the updated options
   */
  public PageStoreOptions setTimeoutDuration(long timeout) {
    mTimeoutDuration = timeout;
    return this;
  }

  /**
   * @return number of threads for handling timeout
   */
  public int getTimeoutThreads() {
    return mTimeoutThreads;
  }

  /**
   * @param threads number of threads for handling timeout
   * @return the updated options
   */
  public PageStoreOptions setTimeoutThreads(int threads) {
    mTimeoutThreads = threads;
    return this;
  }

  /**
   * @return the fraction of space allocated for storage overhead
   */
  public double getOverheadRatio() {
    return mOverheadRatio;
  }

  /**
   * @param overheadRatio the fraction of space allocated for storage overhead
   * @return the updated options
   */
  public PageStoreOptions setOverheadRatio(double overheadRatio) {
    mOverheadRatio = overheadRatio;
    return this;
  }
}
