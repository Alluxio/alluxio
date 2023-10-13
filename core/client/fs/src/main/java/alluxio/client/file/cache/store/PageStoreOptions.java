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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Options used to instantiate a {@link alluxio.client.file.cache.PageStore}.
 */
public class PageStoreOptions {

  /**
   * @param conf configuration
   * @return a list of instance of {@link PageStoreOptions}
   */
  public static List<PageStoreOptions> create(AlluxioConfiguration conf) {
    List<String> dirs = conf.getList(PropertyKey.USER_CLIENT_CACHE_DIRS);
    List<String> cacheSizes = conf.getList(PropertyKey.USER_CLIENT_CACHE_SIZE);
    PageStoreType storeType = conf.getEnum(
        PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.class);
    List<PageStoreOptions> optionsList = createPageStoreOptions(dirs, cacheSizes, storeType);
    optionsList.forEach(options -> {
      options.setFileBuckets(conf.getInt(PropertyKey.USER_CLIENT_CACHE_LOCAL_STORE_FILE_BUCKETS))
          .setPageSize(conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE))
          .setAlluxioVersion(conf.getString(PropertyKey.VERSION))
          .setTimeoutDuration(conf.getMs(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION))
          .setTimeoutThreads(conf.getInt(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_THREADS));
      if (conf.isSet(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD)) {
        options.setOverheadRatio(conf.getDouble(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD));
      }
    });
    return optionsList;
  }

  /**
   * @param conf configuration
   * @return a list of instance of {@link PageStoreOptions}
   */
  public static List<PageStoreOptions> createForWorkerPageStore(AlluxioConfiguration conf) {
    List<String> dirs = conf.getList(PropertyKey.WORKER_PAGE_STORE_DIRS);
    List<String> cacheSizes = conf.getList(PropertyKey.WORKER_PAGE_STORE_SIZES);
    PageStoreType storeType = conf.getEnum(
        PropertyKey.WORKER_PAGE_STORE_TYPE, PageStoreType.class);
    List<PageStoreOptions> optionsList = createPageStoreOptions(dirs, cacheSizes, storeType);
    optionsList.forEach(options -> {
      options.setFileBuckets(conf.getInt(PropertyKey.WORKER_PAGE_STORE_LOCAL_STORE_FILE_BUCKETS))
          .setPageSize(conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE))
          .setAlluxioVersion(conf.getString(PropertyKey.VERSION))
          .setTimeoutDuration(conf.getMs(PropertyKey.WORKER_PAGE_STORE_TIMEOUT_DURATION))
          .setTimeoutThreads(conf.getInt(PropertyKey.WORKER_PAGE_STORE_TIMEOUT_THREADS));
      if (conf.isSet(PropertyKey.WORKER_PAGE_STORE_OVERHEAD)) {
        options.setOverheadRatio(conf.getDouble(PropertyKey.WORKER_PAGE_STORE_OVERHEAD));
      }
    });
    return optionsList;
  }

  private static List<PageStoreOptions> createPageStoreOptions(List<String> dirs,
      List<String> cacheSizes, PageStoreType storeType) {
    Preconditions.checkArgument(!dirs.isEmpty(), "Cache dirs is empty");
    Preconditions.checkArgument(!cacheSizes.isEmpty(), "Cache cacheSizes is empty");
    Preconditions.checkArgument(dirs.size() == cacheSizes.size(),
        "The number of dirs does not match the number of cacheSizes");
    List<PageStoreOptions> optionsList = new ArrayList<>(dirs.size());
    for (int i = 0; i < dirs.size(); i++) {
      PageStoreOptions options = new PageStoreOptions();
      options
          .setRootDir(Paths.get(dirs.get(i), storeType.name()))
          .setCacheSize(FormatUtils.parseSpaceSize(cacheSizes.get(i)))
          .setStoreType(storeType)
          .setOverheadRatio(storeType.getOverheadRatio())
          .setIndex(i);
      optionsList.add(options);
    }
    return optionsList;
  }

  private PageStoreType mStoreType = PageStoreType.LOCAL;
  private int mFileBuckets = 1000;
  /**
   * Root directory where the data is stored.
   */
  private Path mRootDir;

  /**
   * The index of this directory in the list.
   */
  private int mIndex;

  /**
   * Page size for the data.
   */
  private long mPageSize;

  /**
   * Cache size for the data.
   */
  private long mCacheSize;

  /**
   * Alluxio client version.
   */
  private String mAlluxioVersion;

  /**
   * Timeout duration for page store operations in ms.
   */
  private long mTimeoutDuration;

  /**
   * Number of threads for page store operations.
   */
  private int mTimeoutThreads;

  /**
   * A fraction value representing the storage overhead.
   * i.e., with 1GB allocated cache space, and 10% storage overhead we
   * expect no more than 1024MB / (1 + 10%) user data to store
   */
  private double mOverheadRatio;

  /**
   * @return the type corresponding to the page store
   */
  public PageStoreType getType() {
    return mStoreType;
  }

  /**
   * @param rootDir the root directories where pages are stored
   * @return the updated options
   */
  public PageStoreOptions setRootDir(Path rootDir) {
    mRootDir = rootDir;
    return this;
  }

  /**
   * @return the root directory where pages are stored
   */
  public Path getRootDir() {
    return mRootDir;
  }

  /**
   * @param index the index of this directory
   * @return the updated options
   */
  public PageStoreOptions setIndex(int index) {
    mIndex = index;
    return this;
  }

  /**
   * @return the index of this directory
   */
  public int getIndex() {
    return mIndex;
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

  /**
   * @param fileBuckets the number of buckets to place files in
   * @return the updated options
   */
  public PageStoreOptions setFileBuckets(int fileBuckets) {
    mFileBuckets = fileBuckets;
    return this;
  }

  /**
   * @return the number of buckets to place files in
   */
  public int getFileBuckets() {
    return mFileBuckets;
  }

  /**
   * @param storeType
   * @return the updated options
   */
  public PageStoreOptions setStoreType(PageStoreType storeType) {
    mStoreType = storeType;
    return this;
  }
}
