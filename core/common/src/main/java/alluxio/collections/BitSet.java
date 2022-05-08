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

package alluxio.collections;

/**
 * An interface of BitSet supports get/set/clear.
 */
public interface BitSet {

  /**
   * @param index the index of the bit to get
   * @return the bit value of the specified index
   */
  boolean get(int index);

  /**
   * Sets the bit at the specified index to {@code true}.
   *
   * @param index the index of the bit to be set
   */
  void set(int index);

  /**
   * Sets the bit specified by the index to {@code false}.
   *
   * @param index the index of the bit to be cleared
   */
  void clear(int index);

  /**
   * @return the number of bits currently in this bit set
   */
  int size();
}
