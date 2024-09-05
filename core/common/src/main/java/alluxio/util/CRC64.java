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

import java.util.zip.Checksum;

/**
 * THe class for CRC64 calculation. Copied from https://github.com/MrBuddyCasino/crc-64.
 */
public class CRC64 implements Checksum {
  /* 64-bit CRC-ecma182 polynomial with these coefficients, but reversed:
      64, 62, 57, 55, 54, 53, 52, 47, 46, 45, 40, 39, 38, 37, 35, 33, 32,
      31, 29, 27, 24, 23, 22, 21, 19, 17, 13, 12, 10, 9, 7, 4, 1, 0 */
  private final static long POLY = (long) 0xc96c5795d7870f42L; // ECMA-182

  /* CRC64 calculation table. */
  private final static long[][] table;

  /* Current CRC value. */
  private long value;

  static
  {
    /*
     * Nested tables as described by Mark Adler:
     * http://stackoverflow.com/a/20579405/58962
     */
    table = new long[8][256];

    for (int n = 0; n < 256; n++)
    {
      long crc = n;
      for (int k = 0; k < 8; k++)
      {
        if ((crc & 1) == 1)
        {
          crc = (crc >>> 1) ^ POLY;
        }
        else
        {
          crc = (crc >>> 1);
        }
      }
      table[0][n] = crc;
    }

    /* generate nested CRC table for future slice-by-8 lookup */
    for (int n = 0; n < 256; n++)
    {
      long crc = table[0][n];
      for (int k = 1; k < 8; k++)
      {
        crc = table[0][(int) (crc & 0xff)] ^ (crc >>> 8);
        table[k][n] = crc;
      }
    }
  }

  public CRC64() {
    this.value = 0;
  }

  public CRC64(long value) {
    this.value = value;
  }

  public CRC64(byte[] b, int len) {
    this.value = 0;
    update(b, len);
  }

  /**
   * Construct new CRC64 instance from byte array.
   * @param b
   *          the buffer into which the data is read.
   * @return a {@link CRC64} instance.
   **/
  public static CRC64 fromBytes(byte[] b) {
    long l = 0;
    for (int i = 0; i < 4; i++) {
      l <<= 8;
      l ^= (long) b[i] & 0xFF;
    }
    return new CRC64(l);
  }

  /**
   * Get 8 byte representation of current CRC64 value.
   * @return a CRC64 value in 8 byte.
   **/
  public byte[] getBytes() {
    byte[] b = new byte[8];
    for (int i = 0; i < 8; i++) {
      b[7 - i] = (byte) (this.value >>> (i * 8));
    }
    return b;
  }

  /**
   * Get long representation of current CRC64 value.
   * @return a CRC64 value in long type.
   **/
  @Override
  public long getValue() {
    return this.value;
  }

  /**
   * Update CRC64 with new byte block.
   * @param b
   *          the buffer into which the data is read.
   * @param len
   *          the maximum number of bytes to read.
   *
   **/
  public void update(byte[] b, int len) {
    this.update(b, 0, len);
  }

  /**
   * Update CRC64 with new byte.
   * @param b
   *          the byte.
   **/
  public void update(byte b) {
    this.update(new byte[]{b}, 0, 1);
  }

  @Override
  public void update(int b) {
    this.update(new byte[]{(byte)b}, 0, 1);
  }

  @Override
  public void update(byte[] b, int off, int len) {
    this.value = ~this.value;

    /* fast middle processing, 8 bytes (aligned!) per loop */

    int idx = off;
    while (len >= 8)
    {
      value = table[7][(int) (value & 0xff ^ (b[idx] & 0xff))]
          ^ table[6][(int) ((value >>> 8) & 0xff ^ (b[idx + 1] & 0xff))]
          ^ table[5][(int) ((value >>> 16) & 0xff ^ (b[idx + 2] & 0xff))]
          ^ table[4][(int) ((value >>> 24) & 0xff ^ (b[idx + 3] & 0xff))]
          ^ table[3][(int) ((value >>> 32) & 0xff ^ (b[idx + 4] & 0xff))]
          ^ table[2][(int) ((value >>> 40) & 0xff ^ (b[idx + 5] & 0xff))]
          ^ table[1][(int) ((value >>> 48) & 0xff ^ (b[idx + 6] & 0xff))]
          ^ table[0][(int) ((value >>> 56) ^ b[idx + 7] & 0xff)];
      idx += 8;
      len -= 8;
    }

    /* process remaining bytes (can't be larger than 8) */
    while (len > 0)
    {
      value = table[0][(int) ((this.value ^ b[idx]) & 0xff)] ^ (this.value >>> 8);
      idx++;
      len--;
    }

    this.value = ~this.value;
  }

  @Override
  public void reset() {
    this.value = 0;
  }

  private static final int GF2_DIM = 64; /*
   * dimension of GF(2) vectors (length
   * of CRC)
   */

  private static long gf2MatrixTimes(long[] mat, long vec) {
    long sum = 0;
    int idx = 0;
    while (vec != 0) {
      if ((vec & 1) == 1)
        sum ^= mat[idx];
      vec >>>= 1;
      idx++;
    }
    return sum;
  }

  private static void gf2MatrixSquare(long[] square, long[] mat) {
    for (int n = 0; n < GF2_DIM; n++)
      square[n] = gf2MatrixTimes(mat, mat[n]);
  }

  /*
   * Return the CRC-64 of two sequential blocks, where summ1 is the CRC-64 of
   * the first block, summ2 is the CRC-64 of the second block, and len2 is the
   * length of the second block.
   * @param summ1
   *          the {@link CRC64} of the first block.
   * @param summ2
   *          the {@link CRC64} of the second block.
   * @param len2
   *          the length of the second block.
   * @return a {@link CRC64} of two sequential blocks.
   */
  static public CRC64 combine(CRC64 summ1, CRC64 summ2, long len2) {
    // degenerate case.
    if (len2 == 0)
      return new CRC64(summ1.getValue());

    int n;
    long row;
    long[] even = new long[GF2_DIM]; // even-power-of-two zeros operator
    long[] odd = new long[GF2_DIM]; // odd-power-of-two zeros operator

    // put operator for one zero bit in odd
    odd[0] = POLY; // CRC-64 polynomial

    row = 1;
    for (n = 1; n < GF2_DIM; n++) {
      odd[n] = row;
      row <<= 1;
    }

    // put operator for two zero bits in even
    gf2MatrixSquare(even, odd);

    // put operator for four zero bits in odd
    gf2MatrixSquare(odd, even);

    // apply len2 zeros to crc1 (first square will put the operator for one
    // zero byte, eight zero bits, in even)
    long crc1 = summ1.getValue();
    long crc2 = summ2.getValue();
    do {
      // apply zeros operator for this bit of len2
      gf2MatrixSquare(even, odd);
      if ((len2 & 1) == 1)
        crc1 = gf2MatrixTimes(even, crc1);
      len2 >>>= 1;

      // if no more bits set, then done
      if (len2 == 0)
        break;

      // another iteration of the loop with odd and even swapped
      gf2MatrixSquare(odd, even);
      if ((len2 & 1) == 1)
        crc1 = gf2MatrixTimes(odd, crc1);
      len2 >>>= 1;

      // if no more bits set, then done
    } while (len2 != 0);

    // return combined crc.
    crc1 ^= crc2;
    return new CRC64(crc1);
  }

  /*
   * Return the CRC-64 of two sequential blocks, where summ1 is the CRC-64 of
   * the first block, summ2 is the CRC-64 of the second block, and len2 is the
   * length of the second block.
   * @param crc1
   *          the CRC-64 of the first block.
   * @param crc2
   *          the CRC-64 of the second block.
   * @param len2
   *          the length of the second block.
   * @return a CRC-64 of two sequential blocks.
   */
  static public long combine(long crc1, long crc2, long len2) {
    // degenerate case.
    if (len2 == 0)
      return crc1;

    int n;
    long row;
    long[] even = new long[GF2_DIM]; // even-power-of-two zeros operator
    long[] odd = new long[GF2_DIM]; // odd-power-of-two zeros operator

    // put operator for one zero bit in odd
    odd[0] = POLY; // CRC-64 polynomial

    row = 1;
    for (n = 1; n < GF2_DIM; n++) {
      odd[n] = row;
      row <<= 1;
    }

    // put operator for two zero bits in even
    gf2MatrixSquare(even, odd);

    // put operator for four zero bits in odd
    gf2MatrixSquare(odd, even);

    // apply len2 zeros to crc1 (first square will put the operator for one
    // zero byte, eight zero bits, in even)
    do {
      // apply zeros operator for this bit of len2
      gf2MatrixSquare(even, odd);
      if ((len2 & 1) == 1)
        crc1 = gf2MatrixTimes(even, crc1);
      len2 >>>= 1;

      // if no more bits set, then done
      if (len2 == 0)
        break;

      // another iteration of the loop with odd and even swapped
      gf2MatrixSquare(odd, even);
      if ((len2 & 1) == 1)
        crc1 = gf2MatrixTimes(odd, crc1);
      len2 >>>= 1;

      // if no more bits set, then done
    } while (len2 != 0);

    // return combined crc.
    crc1 ^= crc2;
    return crc1;
  }

}
