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

package alluxio.client;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.annotation.PublicApi;

/**
 * Convenience modes for commonly used read types.
 *
 * For finer grained control over data storage, advanced users may specify
 * {@link TachyonStorageType} and {@link UnderStorageType}.
 */
@PublicApi
@ThreadSafe
public enum ReadType {
  /**
   * Read the file and skip Tachyon storage. This read type will not cause any data migration or
   * eviction in Tachyon storage.
   */
  NO_CACHE(1),
  /**
   * Read the file and cache it in the highest tier of a local worker. This read type will not move
   * data between tiers of Tachyon Storage. Users should use {@link #CACHE_PROMOTE} for more
   * optimized performance with tiered storage.
   */
  CACHE(2),
  /**
   * Read the file and cache it in a local worker. Additionally, if the file was in Tachyon
   * storage, it will be promoted to the top storage layer.
   */
  CACHE_PROMOTE(3);

  private final int mValue;

  ReadType(int value) {
    mValue = value;
  }

  /**
   * @return the {@link alluxio.client.TachyonStorageType} associated with this read type
   */
  public TachyonStorageType getTachyonStorageType() {
    if (isPromote()) { // CACHE_PROMOTE
      return TachyonStorageType.PROMOTE;
    }
    if (isCache()) { // CACHE
      return TachyonStorageType.STORE;
    }
    // NO_CACHE
    return TachyonStorageType.NO_STORE;
  }

  /**
   * @return the read type value
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @return true if the read type is {@link #CACHE}, false otherwise
   */
  public boolean isCache() {
    return mValue == CACHE.mValue || mValue == CACHE_PROMOTE.mValue;
  }

  /**
   * @return true if the read type is {@link #CACHE_PROMOTE}, false otherwise
   */
  public boolean isPromote() {
    return mValue == CACHE_PROMOTE.mValue;
  }
}
