/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.io;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

/**
 * Unsafe reader for direct bytebuffer.
 */
public class UnsafeDirectByteBufferReader extends ByteBufferReader {
  private Unsafe mUnsafe;
  private long mBaseOffset;
  private long mOffset;

  public UnsafeDirectByteBufferReader(ByteBuffer buf) throws IOException {
    super(buf);

    if (!buf.isDirect()) {
      throw new IOException("ByteBuffer " + buf + " is not Direct ByteBuffer");
    }
    if (buf.order() != ByteOrder.nativeOrder()) {
      throw new IOException("ByteBuffer " + buf + " has non-native ByteOrder");
    }

    try {
      mUnsafe = UnsafeUtils.getUnsafe();

      Field addressField = Buffer.class.getDeclaredField("address");
      addressField.setAccessible(true);
      mBaseOffset = ((Long)addressField.get(buf)).longValue();
      mOffset = mBaseOffset;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public byte get() {
    return mUnsafe.getByte(mOffset ++);
  }

  @Override
  public void get(byte[] dst) {
    mUnsafe.copyMemory(null, mOffset, dst, UnsafeUtils.sByteArrayBaseOffset, dst.length);
    mOffset += dst.length;
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    mUnsafe.copyMemory(null, mOffset, dst, UnsafeUtils.sByteArrayBaseOffset + offset, length);
    mOffset += length;
  }

  @Override
  public char getChar() {
    mOffset += 2;
    return mUnsafe.getChar(mOffset - 2);
  }

  @Override
  public double getDouble() {
    mOffset += 8;
    return mUnsafe.getDouble(mOffset - 8);
  }

  @Override
  public float getFloat() {
    mOffset += 4;
    return mUnsafe.getFloat(mOffset - 4);
  }

  @Override
  public int getInt() {
    mOffset += 4;
    return mUnsafe.getInt(mOffset - 4);
  }

  @Override
  public long getLong() {
    mOffset += 8;
    return mUnsafe.getLong(mOffset - 8);
  }

  @Override
  public short getShort() {
    mOffset += 2;
    return mUnsafe.getShort(mOffset - 2);
  }

  @Override
  public int position() {
    return (int) (mOffset - mBaseOffset);
  }

  @Override
  public void position(int newPosition) {
    mOffset = mBaseOffset + newPosition;
  }
}
