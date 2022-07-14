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

import com.google.common.base.MoreObjects;

/**
 * Options used to instantiate the {@link LocalPageStore}.
 */
public class LocalPageStoreOptions extends PageStoreOptions {
  // We assume there will be some overhead using local fs as a page store,
  // i.e., with 1GB space allocated, we
  // expect no more than 1024MB / (1 + LOCAL_OVERHEAD_RATIO) logical data stored
  private static final double LOCAL_OVERHEAD_RATIO = 0.05;

  /**
   * The number of file buckets. It is recommended to set this to a high value if the number of
   * unique files is expected to be high (# files / file buckets <= 100,000).
   */
  private int mFileBuckets;

  /**
   * Creates a new instance of {@link LocalPageStoreOptions}.
   */
  public LocalPageStoreOptions() {
    mFileBuckets = 1000;
    mOverheadRatio = LocalPageStoreOptions.LOCAL_OVERHEAD_RATIO;
  }

  /**
   * @param fileBuckets the number of buckets to place files in
   * @return the updated options
   */
  public LocalPageStoreOptions setFileBuckets(int fileBuckets) {
    mFileBuckets = fileBuckets;
    return this;
  }

  /**
   * @return the number of buckets to place files in
   */
  public int getFileBuckets() {
    return mFileBuckets;
  }

  @Override
  public PageStoreType getType() {
    return PageStoreType.LOCAL;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("AlluxioVersion", mAlluxioVersion)
        .add("CacheSize", mCacheSize)
        .add("FileBuckets", mFileBuckets)
        .add("OverheadRatio", mOverheadRatio)
        .add("PageSize", mPageSize)
        .add("RootDir", mRootDir)
        .add("TimeoutDuration", mTimeoutDuration)
        .add("TimeoutThreads", mTimeoutThreads)
        .toString();
  }
}
