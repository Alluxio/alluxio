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
import alluxio.thrift.TWriteTier;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Write Tier writing blocks for a file in Alluxio.
 */
@PublicApi
@ThreadSafe
public enum WriteTier {
  /**
   * Write the blocks to the highest tier in the Alluxio Worker.
   */
  HIGHEST(1),
  /**
   * Write blocks to the second highest tier in an Alluxio Worker. If only tier exists, write to it.
   */
  SECOND_HIGHEST(2),
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
   * Converts client type to thrift type.
   *
   * @param writeTier {@link WriteTier}
   * @return {@link TWriteTier} equivalent
   */
  public static TWriteTier toThrift(WriteTier writeTier) {

    TWriteTier tWriteTier = TWriteTier.Highest;
    if (writeTier != null) {
      switch (writeTier) {
        case HIGHEST:
          tWriteTier = TWriteTier.Highest;
          break;
        case SECOND_HIGHEST:
          tWriteTier = TWriteTier.SecondHighest;
          break;
        case LOWEST:
          tWriteTier = TWriteTier.Lowest;
          break;
        default:
          tWriteTier = TWriteTier.Highest;
          break;
      }
    }
    return tWriteTier;
  }
}
