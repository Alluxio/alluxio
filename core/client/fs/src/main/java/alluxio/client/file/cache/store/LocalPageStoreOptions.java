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

import alluxio.Constants;

import com.google.common.base.MoreObjects;

/**
 * Options used to instantiate the {@link LocalPageStore}.
 */
public class LocalPageStoreOptions extends PageStoreOptions {

  /**
   * The max number of buffers used to transfer data. Total memory usage will be equivalent to
   * {@link #mBufferPoolSize} * {@link #mBufferSize}
   */
  private int mBufferPoolSize;

  /**
   * The size of the buffers used to transfer data. It is recommended to set this at or near the
   * expected max page size.
   */
  private int mBufferSize;

  /**
   * The number of file buckets. It is recommended to set this to a high value if the number of
   * unique files is expected to be high (# files / file buckets <= 100,000).
   */
  private int mFileBuckets;

  /**
   * Creates a new instance of {@link LocalPageStoreOptions}.
   */
  public LocalPageStoreOptions() {
    mBufferPoolSize = 32;
    mBufferSize = Constants.MB;
    mFileBuckets = 1000;
  }

  /**
   * @param bufferPoolSize sets the buffer pool size
   * @return the updated options
   */
  public LocalPageStoreOptions setBufferPoolSize(int bufferPoolSize) {
    mBufferPoolSize = bufferPoolSize;
    return this;
  }

  /**
   * @return the size of the buffer pool
   */
  public int getBufferPoolSize() {
    return mBufferPoolSize;
  }

  /**
   * @param bufferSize the number of buffers in the buffer pool
   * @return the updated options
   */
  public LocalPageStoreOptions setBufferSize(int bufferSize) {
    mBufferSize = bufferSize;
    return this;
  }

  /**
   * @return the number of items
   */
  public int getBufferSize() {
    return mBufferSize;
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
        .add("BufferPoolSize", mBufferPoolSize)
        .add("CacheSize", mCacheSize)
        .add("BufferSize", mBufferSize)
        .add("FileBuckets", mFileBuckets)
        .add("PageSize", mPageSize)
        .add("RootDir", mRootDir)
        .toString();
  }
}
