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

package alluxio.wire;

import alluxio.annotation.PublicApi;
import alluxio.thrift.LoadMetadataTType;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Types for loading metadata.
 */
@PublicApi
@ThreadSafe
public enum LoadMetadataType {
  /**
   * Never loads metadata.
   */
  Never(0),
  /**
   * Loads metadata only at the first time of listing status on a directory.
   */
  Once(1),
  /**
   * Always load metadata when listing status on a directory.
   */
  Always(2),
  ;

  private final int mValue;

  LoadMetadataType(int value) {
    mValue = value;
  }

  /**
   * @return the integer value of the LoadMetadataType
   */
  public int getValue() {
    return mValue;
  }

  /**
   * @param loadMetadataType the {@link LoadMetadataType}
   * @return the thrift representation of this enum
   */
  public static LoadMetadataTType toThrift(LoadMetadataType loadMetadataType) {
    return LoadMetadataTType.findByValue(loadMetadataType.getValue());
  }

  /**
   * @param loadMetadataTType the thrift representation of loadMetadataType
   * @return the {@link LoadMetadataType}
   */
  public static LoadMetadataType fromThrift(LoadMetadataTType loadMetadataTType) {
    switch (loadMetadataTType.getValue()) {
      case 0:
        return Never;
      case 1:
        return Once;
      case 2:
        return Always;
      default:
        return null;
    }
  }
}
