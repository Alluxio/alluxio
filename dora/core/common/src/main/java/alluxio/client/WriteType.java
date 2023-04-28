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

package alluxio.client;

import alluxio.annotation.PublicApi;
import alluxio.grpc.WritePType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Write types for creating a file in Alluxio.
 */
@PublicApi
@ThreadSafe
public enum WriteType {
  /**
   * Write the file, guaranteeing the data is written to Alluxio storage or failing the operation.
   * The data will be written to the highest tier in a worker's storage. Data will not be
   * persisted to the under storage.
   */
  MUST_CACHE(1),
  /**
   * Write the file and try to cache it.
   *
   * @deprecated This write type is deprecated as of v0.8 and not recommended for use. Use either
   * {@link #MUST_CACHE} or {@link #CACHE_THROUGH} depending on your data persistence
   * requirements.
   */
  @Deprecated
  TRY_CACHE(2),
  /**
   * Write the file synchronously to the under fs, and also try to write to the highest tier in a
   * worker's Alluxio storage.
   */
  CACHE_THROUGH(3),
  /**
   * Write the file synchronously to the under fs, skipping Alluxio storage.
   */
  THROUGH(4),
  /**
   * [Experimental] Write the file asynchronously to the under fs.
   */
  ASYNC_THROUGH(5),
  /**
   * Do not store the data in Alluxio or Under Storage. This write type should only be used for
   * development testing.
   */
  NONE(6),
  ;

  private final int mValue;

  WriteType(int value) {
    mValue = value;
  }

  /**
   * @return the {@link AlluxioStorageType} which is associated with this mode
   */
  public AlluxioStorageType getAlluxioStorageType() {
    if (isCache()) {
      return AlluxioStorageType.STORE;
    }
    return AlluxioStorageType.NO_STORE;
  }

  /**
   * @return the {@link alluxio.client.UnderStorageType} which is associated with this mode
   */
  public UnderStorageType getUnderStorageType() {
    if (isThrough()) {
      return UnderStorageType.SYNC_PERSIST;
    } else if (isAsync()) {
      return UnderStorageType.ASYNC_PERSIST;
    }
    return UnderStorageType.NO_PERSIST;
  }

  /**
   * @return the value of the write type
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @return true if by this write type data will be persisted <em>asynchronously</em> to under
   * storage (e.g., {@link #ASYNC_THROUGH}), false otherwise
   */
  public boolean isAsync() {
    return mValue == ASYNC_THROUGH.mValue;
  }

  /**
   * @return true if by this write type data will be cached in Alluxio space (e.g.,
   * {@link #MUST_CACHE}, {@link #CACHE_THROUGH}, {@link #TRY_CACHE}, or
   * {@link #ASYNC_THROUGH}), false otherwise
   */
  public boolean isCache() {
    return (mValue == MUST_CACHE.mValue) || (mValue == CACHE_THROUGH.mValue)
            || (mValue == TRY_CACHE.mValue) || (mValue == ASYNC_THROUGH.mValue);
  }

  /**
   * @return true if by this write type data will be persisted <em>synchronously</em> to under
   * storage (e.g., {@link #CACHE_THROUGH} or {@link #THROUGH}), false otherwise
   */
  public boolean isThrough() {
    return (mValue == CACHE_THROUGH.mValue) || (mValue == THROUGH.mValue);
  }

  /**
   * @param writePType proto type
   * @return wire type for given proto type
   */
  public static WriteType fromProto(WritePType writePType) {
    return WriteType.values()[writePType.getNumber() - 1];
  }

  /**
   * @return proto representation of this instance
   */
  public WritePType toProto() {
    return WritePType.values()[mValue - 1];
  }
}
