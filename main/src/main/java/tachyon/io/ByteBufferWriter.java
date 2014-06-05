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
 * Writer for bytebuffer.
 */
public abstract class ByteBufferWriter {
  /**
   * Get most efficient ByteBufferWriter for the ByteBuffer.
   * 
   * @param buf
   *          the ByteBuffer to write.
   * @return The most efficient ByteBufferWriter for buf.
   * @throws IOException
   */
  public static ByteBufferWriter getByteBufferWriter(ByteBuffer buf) throws IOException {
    // if (buf.order() == ByteOrder.nativeOrder()) {
    // if (buf.isDirect()) {
    // return new UnsafeDirectByteBufferWriter(buf);
    // } else {
    // return new UnsafeHeapByteBufferWriter(buf);
    // }
    // }
    return new JavaByteBufferWriter(buf);
  }

  protected ByteBuffer mBuf;

  ByteBufferWriter(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract ByteBuffer getByteBuffer();

  public ByteOrder order() {
    return mBuf.order();
  }

  /**
   * Writes the given byte into this buffer at the current position, and then increments the
   * position.
   * 
   * @param b
   *          The byte to be written
   */
  public abstract void put(Byte b);

  /**
   * This method transfers the entire content of the given source byte array into this buffer. An
   * invocation of this method of the form <tt>dst.put(a)</tt> behaves in exactly the same way as
   * the invocation
   * 
   * <pre>
   * dst.put(a, 0, a.length)
   * </pre>
   * 
   * @param src
   */
  public final void put(byte[] src) {
    put(src, 0, src.length);
  }

  public abstract void put(byte[] src, int offset, int length);

  public abstract void putChar(char value);

  public abstract void putDouble(double value);

  public abstract void putFloat(float value);

  public abstract void putInt(int value);

  public abstract void putLong(long value);

  public abstract void putShort(short value);
}
