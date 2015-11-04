/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import tachyon.annotation.PublicApi;

/**
 * Convenience modes for commonly used write types for a TachyonFile.
 *
 * For finer grained control over data storage, advanced users may specify
 * {@link tachyon.client.TachyonStorageType} and {@link tachyon.client.UnderStorageType}.
 */
@PublicApi
public enum WriteType {
  /**
   * Write the file, guaranteeing the data is written to Tachyon storage or failing the operation.
   * The data will be written to the highest tier in a worker's storage. Data will not be
   * persisted to the under storage.
   */
  MUST_CACHE(1),
  /**
   * Write the file and try to cache it. This write type is deprecated as of v0.8 and not
   * recommended for use. Use either MUST_CACHE or CACHE_THROUGH depending on your data
   * persistence requirements.
   */
  @Deprecated
  TRY_CACHE(2),
  /**
   * Write the file synchronously to the under fs, and also try to write to the highest tier in a
   * worker's Tachyon storage.
   */
  CACHE_THROUGH(3),
  /**
   * Write the file synchronously to the under fs, skipping Tachyon storage.
   */
  THROUGH(4),
  /**
   * [Experimental] Write the file asynchronously to the under fs (either must cache or must
   * through). This write type is deprecated as of v0.8 and not recommended for use. Use {@link
   * tachyon.client.lineage.TachyonLineageFileSystem} for asynchronous data persistence.
   */
  @Deprecated
  ASYNC_THROUGH(5);

  private final int mValue;

  WriteType(int value) {
    mValue = value;
  }

  /**
   * @return the {@link TachyonStorageType} which is associated with this mode
   */
  public TachyonStorageType getTachyonStorageType() {
    if (isCache()) {
      return TachyonStorageType.STORE;
    }
    return TachyonStorageType.NO_STORE;
  }

  /**
   * @return the {@link tachyon.client.UnderStorageType} which is associated with this mode
   */
  public UnderStorageType getUnderStorageType() {
    if (isThrough()) {
      return UnderStorageType.SYNC_PERSIST;
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
   * This method is deprecated, it is not recommended to use ASYNC_THROUGH. Use {@link
   * tachyon.client.lineage.TachyonLineageFileSystem} for asynchronous data persistence.
   *
   * @return true if the write type is ASYNC_THROUGH, false otherwise
   */
  @Deprecated
  public boolean isAsync() {
    return mValue == ASYNC_THROUGH.mValue;
  }

  /**
   * @return true if the write type is one of MUST_CACHE, CACHE_THROUGH, TRY_CACHE, or ASYNC_THROUGH
   */
  public boolean isCache() {
    return (mValue == MUST_CACHE.mValue) || (mValue == CACHE_THROUGH.mValue)
        || (mValue == TRY_CACHE.mValue) || (mValue == ASYNC_THROUGH.mValue);
  }

  /**
   * @return true if the write type is MUST_CACHE or ASYNC_THROUGH
   */
  public boolean isMustCache() {
    return (mValue == MUST_CACHE.mValue) || (mValue == ASYNC_THROUGH.mValue);
  }

  /**
   * @return true if the write type is CACHE_THROUGH or THROUGH
   */
  public boolean isThrough() {
    return (mValue == CACHE_THROUGH.mValue) || (mValue == THROUGH.mValue);
  }
}
