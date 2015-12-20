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

package tachyon.util.io;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link ByteIOUtils}
 */
public class ByteIOUtilsTest {
  private byte[] mBuf = new byte[1024];

  @Test
  public void readWriteByteTest() throws Exception {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff};
    for (long i : values) {
      byte v = (byte) i;
      ByteIOUtils.writeByte(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readByte(mBuf, 0));
    }
  }

  @Test
  public void readWriteShortTest() throws Exception {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff};
    for (long i : values) {
      short v = (short) i;
      ByteIOUtils.writeShort(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readShort(mBuf, 0));
      ByteIOUtils.writeShort(mBuf, 1, v);
      Assert.assertEquals(v, ByteIOUtils.readShort(mBuf, 1));
    }
  }

  @Test
  public void readWriteIntTest() throws Exception {
    long[] values = new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff};
    for (long i : values) {
      int v = (int) i;
      ByteIOUtils.writeInt(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readInt(mBuf, 0));
      ByteIOUtils.writeInt(mBuf, 1, v);
      Assert.assertEquals(v, ByteIOUtils.readInt(mBuf, 1));
      ByteIOUtils.writeInt(mBuf, 2, v);
      Assert.assertEquals(v, ByteIOUtils.readInt(mBuf, 2));
      ByteIOUtils.writeInt(mBuf, 3, v);
      Assert.assertEquals(v, ByteIOUtils.readInt(mBuf, 3));
    }
  }

  @Test
  public void readWriteLongTest() throws Exception {
    long[] values =
        new long[] {0, 1, 2, 0x7f, 0xff, 0xffff, 0xffffff, 0xffffffff, 0xffffffffffL,
            0xffffffffffffL, 0xffffffffffffffL, 0xffffffffffffffffL};
    for (long v : values) {
      ByteIOUtils.writeLong(mBuf, 0, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 0));
      ByteIOUtils.writeLong(mBuf, 1, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 1));
      ByteIOUtils.writeLong(mBuf, 2, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 2));
      ByteIOUtils.writeLong(mBuf, 3, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 3));
      ByteIOUtils.writeLong(mBuf, 4, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 4));
      ByteIOUtils.writeLong(mBuf, 5, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 5));
      ByteIOUtils.writeLong(mBuf, 6, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 6));
      ByteIOUtils.writeLong(mBuf, 7, v);
      Assert.assertEquals(v, ByteIOUtils.readLong(mBuf, 7));
    }
  }
}
