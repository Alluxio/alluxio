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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.AlluxioStorageType;
import alluxio.client.ReadType;
import alluxio.client.file.policy.FileWriteLocationPolicy;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for reading a file.
 */
@PublicApi
@NotThreadSafe
public final class InStreamOptions {
  private FileWriteLocationPolicy mLocationPolicy;
  private ReadType mReadType;
  /** Cache incomplete blocks if Alluxio is configured to store blocks in Alluxio storage. */
  private boolean mCachePartiallyReadBlock;
  /**
   * The cache read buffer size in seek. This is only used if {@link #mCachePartiallyReadBlock}
   * is enabled.
   */
  private long mSeekBufferSizeBytes;

  /**
   * @return the default {@link InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new InStreamOptions();
  }

  private InStreamOptions() {
    mReadType = Configuration.getEnum(PropertyKey.USER_FILE_READ_TYPE_DEFAULT, ReadType.class);
    try {
      mLocationPolicy = CommonUtils.createNewClassInstance(
          Configuration.<FileWriteLocationPolicy>getClass(
              PropertyKey.USER_FILE_WRITE_LOCATION_POLICY), new Class[] {}, new Object[] {});
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    mCachePartiallyReadBlock =
        Configuration.getBoolean(PropertyKey.USER_FILE_CACHE_PARTIALLY_READ_BLOCK);
    mSeekBufferSizeBytes =
        Configuration.getBytes(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES);
  }

  /**
   * @return the location policy to use when storing data to Alluxio
   */
  public FileWriteLocationPolicy getLocationPolicy() {
    return mLocationPolicy;
  }

  /**
   * @return the Alluxio storage type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    return mReadType.getAlluxioStorageType();
  }

  /**
   * @param policy the location policy to use when storing data to Alluxio
   * @return the updated options object
   */
  public InStreamOptions setLocationPolicy(FileWriteLocationPolicy policy) {
    mLocationPolicy = policy;
    return this;
  }

  /**
   * Sets the {@link ReadType}.
   *
   * @param readType the {@link ReadType} for this operation. Setting this will override the
   *        {@link AlluxioStorageType}.
   * @return the updated options object
   */
  public InStreamOptions setReadType(ReadType readType) {
    mReadType = readType;
    return this;
  }

  /**
   * @return true if incomplete block caching is enabled
   */
  public boolean isCachePartiallyReadBlock() {
    return mCachePartiallyReadBlock;
  }

  /**
   * Enables/Disables incomplete block caching.
   *
   * @param cachePartiallyReadBlock set to true if to enable incomplete block caching
   * @return the updated options object
   */
  public InStreamOptions setCachePartiallyReadBlock(boolean cachePartiallyReadBlock) {
    mCachePartiallyReadBlock = cachePartiallyReadBlock;
    return this;
  }

  /**
   * @return the seek buffer size in bytes
   */
  public long getSeekBufferSizeBytes() {
    return mSeekBufferSizeBytes;
  }

  /**
   * Sets {@link #mSeekBufferSizeBytes}.
   * @param bufferSizeBytes the seek buffer size
   * @return the updated ooptions object
   */
  public InStreamOptions setSeekBufferSizeBytes(long bufferSizeBytes) {
    mSeekBufferSizeBytes = bufferSizeBytes;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InStreamOptions)) {
      return false;
    }
    InStreamOptions that = (InStreamOptions) o;
    return Objects.equal(mLocationPolicy, that.mLocationPolicy)
        && Objects.equal(mReadType, that.mReadType)
        && Objects.equal(mCachePartiallyReadBlock, that.mCachePartiallyReadBlock)
        && Objects.equal(mSeekBufferSizeBytes, that.mSeekBufferSizeBytes);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mLocationPolicy, mReadType, mCachePartiallyReadBlock, mSeekBufferSizeBytes);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("locationPolicy", mLocationPolicy)
        .add("readType", mReadType).add("cachePartiallyReadBlock", mCachePartiallyReadBlock)
        .add("seekBufferSize", mSeekBufferSizeBytes).toString();
  }
}
