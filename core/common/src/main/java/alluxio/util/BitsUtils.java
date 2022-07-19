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

public class BitsUtils {
  public static long mergeWord(long a, long b, int fromBitIndex, int toBitIndex) {
    long mask = genMask(fromBitIndex, toBitIndex);
    long v = b << fromBitIndex;
    return mergeWord(a, v, mask);
  }

  public static long getValueFromWord(long word, int fromBitIndex, int toBitIndex) {
    return ((word & genMask(fromBitIndex, toBitIndex)) >>> fromBitIndex);
  }

  public static long genMask(int fromBitIndex, long toBitIndex) {
    long l1 = -1L << fromBitIndex;
    long l2 = (toBitIndex >= 64) ? (0L) : (-1L << toBitIndex);
    return (l1 ^ l2);
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
   * @param a    value to merge in non-masked bits
   * @param b    value to merge in masked bits
   * @param mask 1 where bits from b should be selected; 0 where from a
   * @return result of (a & ~mask) | (b & mask)
   */
  public static long mergeWordOpt(long a, long b, long mask) {
    return a ^ ((a ^ b) & mask);
  }

  public static boolean hasZero4(long x) {
    return hasZero4Internal(x) != 0;
  }

  public static boolean hasZero8(long x) {
    return hasZero8Internal(x) != 0;
  }

  public static boolean hasZero12(long x) {
    return hasZero12Internal(x) != 0;
  }

  public static boolean hasZero16(long x) {
    return hasZero16Internal(x) != 0;
  }

  public static boolean hasValue4(long x, int n) {
    return (hasZero4Internal((x) ^ (0x1111L * (n)))) != 0;
  }

  public static boolean hasValue8(long x, int n) {
    return (hasZero8Internal((x) ^ (0x01010101L * (n)))) != 0;
  }

  public static boolean hasValue12(long x, int n) {
    return (hasZero12Internal((x) ^ (0x001001001001L * (n)))) != 0;
  }

  public static boolean hasValue16(long x, int n) {
    return (hasZero16Internal((x) ^ (0x0001000100010001L * (n)))) != 0;
  }

  private static long hasZero4Internal(long x) {
    return (((x) - 0x1111L) & (~(x)) & 0x8888L);
  }

  private static long hasZero8Internal(long x) {
    return (((x) - 0x01010101L) & (~(x)) & 0x80808080L);
  }

  private static long hasZero12Internal(long x) {
    return (((x) - 0x001001001001L) & (~(x)) & 0x800800800800L);
  }

  private static long hasZero16Internal(long x) {
    return (((x) - 0x0001000100010001L) & (~(x)) & 0x8000800080008000L);
  }
}
