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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

/**
 * Unsafe reader for bytebuffer with backing array.
 */
public class UnsafeHeapByteBufferReader extends ByteBufferReader {
  private Unsafe mUnsafe;
  private long mBaseOffset;
  private long mOffset;
  private byte[] mArr;

  public UnsafeHeapByteBufferReader(ByteBuffer buf) throws IOException {
    super(buf);

    if (!buf.hasArray()) {
      throw new IOException("ByteBuffer " + buf + " does not have backing array");
    }
    if (buf.order() != ByteOrder.nativeOrder()) {
      throw new IOException("ByteBuffer " + buf + " has non-native ByteOrder");
    }

    mArr = buf.array();

    try {
      mUnsafe = UnsafeUtils.getUnsafe(); 
    } catch (Exception e) {
      throw new IOException(e);
    }

    mBaseOffset = mUnsafe.arrayBaseOffset(byte[].class);
    mOffset = mBaseOffset;
  }

  @Override
  public byte get() {
    return mUnsafe.getByte(mArr, mOffset ++);
  }

  @Override
  public void get(byte[] dst) {
    mUnsafe.copyMemory(mArr, mOffset, dst, mBaseOffset, dst.length);
    mOffset += dst.length;
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    mUnsafe.copyMemory(mArr, mOffset, dst, mBaseOffset + offset, length);
    mOffset += length;
  }

  @Override
  public char getChar() {
    mOffset += 2;
    return mUnsafe.getChar(mArr, mOffset - 2);
  }

  @Override
  public double getDouble() {
    mOffset += 8;
    return mUnsafe.getDouble(mArr, mOffset - 8);
  }

  @Override
  public float getFloat() {
    mOffset += 4;
    return mUnsafe.getFloat(mArr, mOffset - 4);
  }

  @Override
  public int getInt() {
    mOffset += 4;
    return mUnsafe.getInt(mArr, mOffset - 4);
  }

  @Override
  public long getLong() {
    mOffset += 8;
    return mUnsafe.getLong(mArr, mOffset - 8);
  }

  @Override
  public short getShort() {
    mOffset += 2;
    return mUnsafe.getShort(mArr, mOffset - 2);
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
