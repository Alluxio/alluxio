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
import alluxio.grpc.ReadPType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Convenience modes for commonly used read types.
 *
 * For finer grained control over data storage, advanced users may specify
 * {@link AlluxioStorageType} and {@link UnderStorageType}.
 */
@PublicApi
@ThreadSafe
public enum ReadType {
  /**
   * Read the file and skip Alluxio storage. This read type will not cause any data migration or
   * eviction in Alluxio storage.
   */
  NO_CACHE(1),
  /**
   * Read the file and cache it in the highest tier of a local worker. This read type will not move
   * data between tiers of Alluxio Storage. Users should use {@link #CACHE_PROMOTE} for more
   * optimized performance with tiered storage.
   */
  CACHE(2),
  /**
   * Read the file and cache it in a local worker. Additionally, if the file was in Alluxio
   * storage, it will be promoted to the top storage layer.
   */
  CACHE_PROMOTE(3),
  ;

  private final int mValue;

  ReadType(int value) {
    mValue = value;
  }

  /**
   * @return the {@link AlluxioStorageType} associated with this read type
   */
  public AlluxioStorageType getAlluxioStorageType() {
    if (isPromote()) { // CACHE_PROMOTE
      return AlluxioStorageType.PROMOTE;
    }
    if (isCache()) { // CACHE
      return AlluxioStorageType.STORE;
    }
    // NO_CACHE
    return AlluxioStorageType.NO_STORE;
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

  /**
   * @param readPType proto type
   * @return wire type for given proto type
   */
  public static ReadType fromProto(ReadPType readPType) {
    return ReadType.values()[readPType.getNumber() - 1];
  }

  /**
   * @return proto representation of this instance
   */
  public ReadPType toProto() {
    return ReadPType.values()[mValue - 1];
  }

  /**
   * Creates an instance type from the string. This method is case insensitive.
   *
   * @param text the instance type in string
   * @return the created instance
   */
  public static ReadType fromString(String text) {
    for (ReadType type : ReadType.values()) {
      if (type.toString().equalsIgnoreCase(text)) {
        return type;
      }
    }
    throw new IllegalArgumentException("No constant with text " + text + " found");
  }
}
