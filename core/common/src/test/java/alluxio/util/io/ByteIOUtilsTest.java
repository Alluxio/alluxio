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

package alluxio.util.io;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Unit tests for {@link ByteIOUtils}.
 */
public final class ByteIOUtilsTest {
  private byte[] mBuf = new byte[1024];

  /**
   * Tests {@link ByteIOUtils#writeByte} and {@link ByteIOUtils#readByte}.
   */
  @Test
  public void readWriteByte() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff};
    for (long i : values) {
      byte v = (byte) i;
      ByteIOUtils.writeByte(mBuf, 0, v);
      assertEquals(v, ByteIOUtils.readByte(mBuf, 0));
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeShort} and {@link ByteIOUtils#readShort}.
   */
  @Test
  public void readWriteShort() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff};
    for (long i : values) {
      short v = (short) i;
      ByteIOUtils.writeShort(mBuf, 0, v);
      assertEquals(v, ByteIOUtils.readShort(mBuf, 0));
      ByteIOUtils.writeShort(mBuf, 1, v);
      assertEquals(v, ByteIOUtils.readShort(mBuf, 1));
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeInt} and {@link ByteIOUtils#readInt}.
   */
  @Test
  public void readWriteInt() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff};
    for (long i : values) {
      int v = (int) i;
      for (int pos = 0; pos < 4; pos++) {
        ByteIOUtils.writeInt(mBuf, pos, v);
        assertEquals(v, ByteIOUtils.readInt(mBuf, pos));
      }
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeLong} and {@link ByteIOUtils#readLong}.
   */
  @Test
  public void readWriteLong() {
    long[] values =
        new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff, 0xffffffffffL,
            0xffffffffffffL, 0xffffffffffffffL, 0xffffffffffffffffL};
    for (long v : values) {
      for (int pos = 0; pos < 8; pos++) {
        ByteIOUtils.writeLong(mBuf, 0, v);
        assertEquals(v, ByteIOUtils.readLong(mBuf, 0));
      }
    }
  }
}
