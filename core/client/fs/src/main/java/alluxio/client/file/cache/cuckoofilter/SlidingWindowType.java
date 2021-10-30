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

package alluxio.client.file.cache.cuckoofilter;

/**
 * This class represents the type of sliding window.
 */
public enum SlidingWindowType {
  /**
   * The definition of count-based sliding window from "Sliding Sketches: A Framework using Time
   * Zones for Data * Stream Processing in Sliding Windows" by Gou et al.: Given a data stream S, a
   * count-based sliding window with length N means the union of the last N items.
   *
   */
  COUNT_BASED(0),
  /**
   * The definition of time-based sliding window from "Sliding Sketches: A Framework using Time
   * Zones for Data Stream Processing in Sliding Windows" by Gou et al.: Given a data stream S, a
   * time-based sliding window with length N means the union of data items which arrive in the last
   * N time units.
   */
  TIME_BASED(1),
  /**
   * NONE indicates that sliding window model is not used.
   */
  NONE(2);

  private final int mType;

  SlidingWindowType(int type) {
    mType = type;
  }

  /**
   * @return the type of sliding window
   */
  public int getType() {
    return mType;
  }
}
