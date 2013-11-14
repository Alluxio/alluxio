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
 * Unsafe writer for bytebuffer with backing array.
 */
public class UnsafeHeapByteBufferWriter extends ByteBufferWriter {
  private Unsafe mUnsafe;
  private long mBaseOffset;
  private long mOffset;
  private byte[] mArr;

  UnsafeHeapByteBufferWriter(ByteBuffer buf) throws IOException {
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
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | 
        IllegalAccessException e) {
      throw new IOException(e);
    }

    mBaseOffset = mUnsafe.arrayBaseOffset(byte[].class);
    mOffset = mBaseOffset;
  }

  @Override
  public void put(Byte b) {
    mUnsafe.putByte(mArr, mOffset ++, b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    mUnsafe.copyMemory(src, mBaseOffset + offset, mArr, mOffset, length);
    mOffset += length;
  }

  @Override
  public void putChar(char value) {
    mUnsafe.putChar(mArr, mOffset, value);
    mOffset += 2;
  }

  @Override
  public void putDouble(double value) {
    mUnsafe.putDouble(mArr, mOffset, value);
    mOffset += 8;
  }

  @Override
  public void putFloat(float value) {
    mUnsafe.putFloat(mArr, mOffset, value);
    mOffset += 4;
  }

  @Override
  public void putInt(int value) {
    mUnsafe.putInt(mArr, mOffset, value);
    mOffset += 4;
  }

  @Override
  public void putLong(long value) {
    mUnsafe.putLong(mArr, mOffset, value);
    mOffset += 8;
  }

  @Override
  public void putShort(short value) {
    mUnsafe.putShort(mArr, mOffset, value);
    mOffset += 2;
  }

  @Override
  public ByteBuffer getByteBuffer() {
    ByteBuffer buf = mBuf.duplicate();
    buf.position(0);
    buf.limit((int) (mOffset - mBaseOffset));
    buf.order(mBuf.order());
    return buf;
  }
}
