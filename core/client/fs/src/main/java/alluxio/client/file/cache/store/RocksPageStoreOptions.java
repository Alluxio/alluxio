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
import alluxio.proto.client.Cache.PPageStoreCommonOptions;
import alluxio.proto.client.Cache.PRocksPageStoreOptions;

import com.google.common.base.MoreObjects;
import org.rocksdb.CompressionType;

/**
 * Options used to instantiate {@link RocksPageStore}.
 */
public class RocksPageStoreOptions extends PageStoreOptions {
  // TODO(feng): consider making the overhead ratio configurable
  // We assume 20% overhead using Rocksdb as a page store, i.e., with 1GB space allocated, we
  // expect no more than 1024MB/(1+20%)=853MB logical data stored
  private static final double ROCKS_OVERHEAD_RATIO = 0.2;

  /** The max page size that can be stored. */
  private int mMaxPageSize;

  /** The maximum size of the write buffer in the rocksDB. */
  private int mWriteBufferSize;

  /** The maximum number of buffers of {@link #mMaxPageSize} size that will be used. */
  private int mMaxBufferPoolSize;

  /** The type of compression to use in the DB. */
  private CompressionType mCompressionType;

  /**
   * Creates a new instance of {@link RocksPageStoreOptions}.
   */
  public RocksPageStoreOptions() {
    mRootDir = "";
    mMaxPageSize = Constants.MB;
    mWriteBufferSize = 64 * Constants.MB;
    mMaxBufferPoolSize = 32;
    mCompressionType = CompressionType.NO_COMPRESSION;
    mOverheadRatio = ROCKS_OVERHEAD_RATIO;
  }

  /**
   * @param maxPageSize the max page size that can be stored
   * @return the updated options
   */
  public RocksPageStoreOptions setMaxPageSize(int maxPageSize) {
    mMaxPageSize = maxPageSize;
    return this;
  }

  /**
   * @return the max page size
   */
  public int getMaxPageSize() {
    return mMaxPageSize;
  }

  /**
   * @param writeBufferSize the size of the rocksDB write buffer
   * @return the updated options
   */
  public RocksPageStoreOptions setWriteBufferSize(int writeBufferSize) {
    mWriteBufferSize = writeBufferSize;
    return this;
  }

  /**
   * @return the rocksDB write buffer size
   */
  public int getWriteBufferSize() {
    return mWriteBufferSize;
  }

  /**
   * @param bufferPoolSize the max number of buffers in the buffer pool
   * @return the updated options
   */
  public RocksPageStoreOptions setMaxBufferPoolSize(int bufferPoolSize) {
    mMaxBufferPoolSize = bufferPoolSize;
    return this;
  }

  /**
   * @return the max number of buffers in the buffer pool
   */
  public int getMaxBufferPoolSize() {
    return mMaxBufferPoolSize;
  }

  /**
   * @param type the compression type for rocksDB
   * @return the updated options
   */
  public RocksPageStoreOptions setCompressionType(CompressionType type) {
    mCompressionType = type;
    return this;
  }

  /**
   * @return the compression for rocksDB
   */
  public CompressionType getCompressionType() {
    return mCompressionType;
  }

  @Override
  public PageStoreType getType() {
    return PageStoreType.ROCKS;
  }

  /**
   * @return protobuf representation of the options
   */
  public PRocksPageStoreOptions toProto() {
    return PRocksPageStoreOptions.newBuilder()
        .setCommonOptions(PPageStoreCommonOptions.newBuilder()
            .setPageSize(getPageSize())
            .setCacheSize(getCacheSize())
            .setAlluxioVersion(getAlluxioVersion())
        )
        .build();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("AlluxioVersion", mAlluxioVersion)
        .add("CacheSize", mCacheSize)
        .add("CompressionType", mCompressionType)
        .add("MaxBufferPoolSize", mMaxBufferPoolSize)
        .add("MaxPageSize", mMaxPageSize)
        .add("OverheadRatio", mOverheadRatio)
        .add("PageSize", mPageSize)
        .add("RootDir", mRootDir)
        .add("TimeoutDuration", mTimeoutDuration)
        .add("TimeoutThreads", mTimeoutThreads)
        .add("WriteBufferSize", mWriteBufferSize)
        .toString();
  }
}
