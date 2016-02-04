/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.util.io;

import org.junit.Assert;
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
  public void readWriteByteTest() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff};
    for (long i : values) {
      byte v = (byte) i;
      ByteIOUtils.writeByte(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readByte(mBuf, 0));
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeShort} and {@link ByteIOUtils#readShort}.
   */
  @Test
  public void readWriteShortTest() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff};
    for (long i : values) {
      short v = (short) i;
      ByteIOUtils.writeShort(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readShort(mBuf, 0));
      ByteIOUtils.writeShort(mBuf, 1, v);
      Assert.assertEquals(v, ByteIOUtils.readShort(mBuf, 1));
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeInt} and {@link ByteIOUtils#readInt}.
   */
  @Test
  public void readWriteIntTest() {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff};
    for (long i : values) {
      int v = (int) i;
      for (int pos = 0; pos < 4; pos ++) {
        ByteIOUtils.writeInt(mBuf, pos, v);
        Assert.assertEquals(v, ByteIOUtils.readInt(mBuf, pos));
      }
    }
  }

  /**
   * Tests {@link ByteIOUtils#writeLong} and {@link ByteIOUtils#readLong}.
   */
  @Test
  public void readWriteLongTest() {
    long[] values =
        new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff, 0xffffffffffL,
            0xffffffffffffL, 0xffffffffffffffL, 0xffffffffffffffffL};
    for (long v : values) {
      for (int pos = 0; pos < 8; pos ++) {
        ByteIOUtils.writeLong(mBuf, 0, v);
        Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 0));
      }
    }
  }
}
