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
import alluxio.thrift.TTierPolicy;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Write Tier policy when writing blocks for a file in Alluxio.
 */
@PublicApi
@ThreadSafe
public enum WriteTier{
  /**
   * Write the blocks to the highest tier in the Alluxio Worker.
   */
  HIGHEST(1),
  /**
   * Preference to write blocks to the highest non-memory tier in an Alluxio Worker.
   */
  PREFER_HIGHEST_NON_MEMORY(2),
  /**
   * Write the blocks to the lowest tier in an Alluxio Worker.
   */
  LOWEST(3),
  ;

  private final int mValue;

  WriteTier(int value) {
    mValue = value;
  }

  /**
   * @return the value of the write type
   */
  public int getValue() {
    return mValue;
  }

  /**
   * Converts thrift type to client type.
   *
   * @param tTierPolicy {@link TTierPolicy}
   * @return {@link WriteTier} equivalent
   */
  public static WriteTier fromThrift(TTierPolicy tTierPolicy) {

    WriteTier tierPolicy = WriteTier.HIGHEST;
    if (tTierPolicy != null) {
      switch (tTierPolicy) {
        case Highest:
          tierPolicy = WriteTier.HIGHEST;
          break;
        case PreferHighestNonMemory:
          tierPolicy = WriteTier.PREFER_HIGHEST_NON_MEMORY;
          break;
        case Lowest:
          tierPolicy = WriteTier.LOWEST;
          break;
        default:
          tierPolicy = WriteTier.HIGHEST;
          break;
      }
    }
    return tierPolicy;
  }

  /**
   * Converts client type to thrift type.
   *
   * @param writeTier {@link WriteTier}
   * @return {@link TTierPolicy} equivalent
   */
  public static TTierPolicy toThrift(WriteTier writeTier) {

    TTierPolicy tTierPolicy = TTierPolicy.Highest;
    if (writeTier != null) {
      switch (writeTier) {
        case HIGHEST:
          tTierPolicy = TTierPolicy.Highest;
          break;
        case PREFER_HIGHEST_NON_MEMORY:
          tTierPolicy = TTierPolicy.PreferHighestNonMemory;
          break;
        case LOWEST:
          tTierPolicy = TTierPolicy.Lowest;
          break;
        default:
          tTierPolicy = TTierPolicy.Highest;
          break;
      }
    }
    return tTierPolicy;
  }
}
