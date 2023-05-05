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

package alluxio.worker.block.annotator;

import java.util.Comparator;

/**
 * Enumeration class to specify natural & reverse orders
 * along with some utilities.
 */
public enum BlockOrder {
  NATURAL, REVERSE;

  /**
   * @return the opposite of the order
   */
  public BlockOrder reversed() {
    switch (this) {
      case NATURAL:
        return BlockOrder.REVERSE;
      case REVERSE:
        return BlockOrder.NATURAL;
      default:
        throw new IllegalArgumentException(String.format("Undefined block order:%s", this));
    }
  }

  /**
   * @return the comparator for the order
   */
  public Comparator<Comparable> comparator() {
    switch (this) {
      case NATURAL:
        return Comparator.naturalOrder();
      case REVERSE:
        return Comparator.reverseOrder();
      default:
        throw new IllegalArgumentException(String.format("Undefined block order:%s", this));
    }
  }
}
