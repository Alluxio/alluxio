/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Reader for bytebuffer.
 */
public abstract class ByteBufferReader {
  /**
   * Get most efficient ByteBufferReader for the ByteBuffer.
   * 
   * @param buf
   *          the ByteBuffer to read.
   * @return The most efficient ByteBufferReader for buf.
   * @throws IOException
   */
  public static ByteBufferReader getByteBufferReader(ByteBuffer buf) throws IOException {
    // if (buf.order() == ByteOrder.nativeOrder()) {
    // if (buf.isDirect()) {
    // return new UnsafeDirectByteBufferReader(buf);
    // } else {
    // return new UnsafeHeapByteBufferReader(buf);
    // }
    // }
    return new JavaByteBufferReader(buf);
  }

  protected ByteBuffer mBuf;

  ByteBufferReader(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract byte get();

  public abstract void get(byte[] dst);

  public abstract void get(byte[] dst, int offset, int length);

  public abstract char getChar();

  public abstract double getDouble();

  public abstract float getFloat();

  public abstract int getInt();

  public abstract long getLong();

  public abstract short getShort();

  public ByteOrder order() {
    return mBuf.order();
  }

  public void order(ByteOrder bo) {
    mBuf.order(bo);
  }

  public abstract int position();

  public abstract void position(int newPosition);
}
