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
  NEVER(0),
  /**
   * Loads metadata only at the first time of listing status on a directory.
   */
  ONCE(1),
  /**
   * Always load metadata when listing status on a directory.
   */
  ALWAYS(2),
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
}
