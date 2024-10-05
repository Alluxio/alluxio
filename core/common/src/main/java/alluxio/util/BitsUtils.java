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

package alluxio.util;

/**
 * CuckooBitSet utility.
 */
public class BitsUtils {
  /**
   * Merge word b into word a with given range.
   * @param a target word
   * @param b source word
   * @param fromBitIndex start bit index
   * @param toBitIndex end bit index
   * @return the merged value
   */
  public static long mergeWord(long a, long b, int fromBitIndex, int toBitIndex) {
    long mask = genMask(fromBitIndex, toBitIndex);
    long v = b << fromBitIndex;
    return mergeWord(a, v, mask);
  }

  /**
   * @param a    value to merge in non-masked bits
   * @param b    value to merge in masked bits
   * @param mask 1 where bits from b should be selected; 0 where from a
   * @return result of (a & ~mask) | (b & mask) goes here
   */
  public static long mergeWord(long a, long b, long mask) {
    return (a & ~mask) | (b & mask);
  }

  /**
   * Get the value from a word with given range.
   * @param word the word to get the value from
   * @param fromBitIndex the start bit index
   * @param toBitIndex the end bit index
   * @return the value from the word
   */
  public static long getValueFromWord(long word, int fromBitIndex, int toBitIndex) {
    return ((word & genMask(fromBitIndex, toBitIndex)) >>> fromBitIndex);
  }

  /**
   * Generate mask from fromBitIndex to toBitIndex.
   * @param fromBitIndex start bit index
   * @param toBitIndex end bit index
   * @return mask
   */
  public static long genMask(int fromBitIndex, long toBitIndex) {
    long l1 = -1L << fromBitIndex;
    long l2 = (toBitIndex >= 64) ? (0L) : (-1L << toBitIndex);
    return (l1 ^ l2);
  }
}
