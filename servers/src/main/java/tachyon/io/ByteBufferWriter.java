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

package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Writer for <code>ByteBuffer</code>.
 */
public abstract class ByteBufferWriter {
  /**
   * Gets the most efficient <code>ByteBufferWriter</code> for the <code>ByteBuffer</code>.
   *
   * @param buf the <code>ByteBuffer</code> to write
   * @return the most efficient <code>ByteBufferWriter</code> for buf
   * @throws IOException if the given buffer is null
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

  /**
   * @param buf <code>ByteBuffer</code> to write
   * @throws IOException if the given buffer is null
   */
  ByteBufferWriter(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  public abstract ByteBuffer getByteBuffer();

  /**
   * @return <code>ByteOrder</code> of the underlying buffer
   */
  public ByteOrder order() {
    return mBuf.order();
  }

  /**
   * Writes the given <code>byte</code> into the buffer.
   *
   * @param b the <code>byte</code> to be write
   */
  public abstract void put(Byte b);

  /**
   * Writes the given byte array into the buffer.
   *
   * An invocation of this method of the form <tt>dst.put(a)</tt> behaves in exactly the same way as
   * the invocation <tt>dst.put(a, 0, a.length)</tt>.
   *
   * @param src the byte array to read bytes from
   */
  public final void put(byte[] src) {
    put(src, 0, src.length);
  }

  /**
   * Writes the <tt>src[offset]</tt> through <tt>src[offest + length - 1]</tt> portion of given byte
   * array into the buffer.
   *
   * @param src the byte array to read bytes from
   * @param offset the offset to start the operation at
   * @param length the number of bytes to transfer
   */
  public abstract void put(byte[] src, int offset, int length);

  /**
   * Writes the given <code>char</code> into the buffer.
   *
   * @param value the <code>char</code> to write
   */
  public abstract void putChar(char value);

  /**
   * Writes the given <code>double</code> into the buffer.
   *
   * @param value the <code>double</code> to write
   */
  public abstract void putDouble(double value);

  /**
   * Writes the given <code>float</code> into the buffer.
   *
   * @param value the <code>float</code> to write
   */
  public abstract void putFloat(float value);

  /**
   * Writes the given <code>int</code> into the buffer.
   *
   * @param value the <code>int</code> to write
   */
  public abstract void putInt(int value);

  /**
   * Writes the given <code>long</code> into the buffer.
   *
   * @param value the <code>long</code> to write
   */
  public abstract void putLong(long value);

  /**
   * Writes the given <code>short</code> into the buffer.
   *
   * @param value the <code>short</code> to write
   */
  public abstract void putShort(short value);
}
