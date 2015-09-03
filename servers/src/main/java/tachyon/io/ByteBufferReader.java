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
 * Reader for a <code>ByteBuffer</code>.
 */
public abstract class ByteBufferReader {
  /**
   * Gets the most efficient <code>ByteBufferReader</code> for the <code>ByteBuffer</code>.
   *
   * @param buf the <code>ByteBuffer</code> to read
   * @return the most efficient <code>ByteBufferReader</code> for the given buffer
   * @throws IOException if the given buffer is null
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

  /**
   * @param buf <code>ByteBuffer</code> to read
   * @throws IOException if the given buffer is null
   */
  ByteBufferReader(ByteBuffer buf) throws IOException {
    if (buf == null) {
      throw new IOException("ByteBuffer is null");
    }

    mBuf = buf;
  }

  /**
   * Reads a <code>byte</code> from the buffer.
   *
   * @return the <code>byte</code> read
   */
  public abstract byte get();

  /**
   * Reads a sequence of bytes from the buffer into the given byte array.
   *
   * @param dst the byte array to write to
   */
  public abstract void get(byte[] dst);

  /**
   * Reads a sequence of bytes from the buffer into the <tt>dst[offset]</tt> through
   * <tt>dst[offest + length - 1]</tt> portion of the given byte array.
   *
   * @param dst the byte array to write to
   * @param offset the offset to start the operation at
   * @param length the number of bytes to transfer
   */
  public abstract void get(byte[] dst, int offset, int length);

  /**
   * Reads a <code>char</code> from the buffer.
   *
   * @return the <code>char</code> read
   */
  public abstract char getChar();

  /**
   * Reads a <code>double</code> from the buffer.
   *
   * @return the <code>double</code> read
   */
  public abstract double getDouble();

  /**
   * Reads a <code>float</code> from the buffer.
   *
   * @return the <code>floar</code> read
   */
  public abstract float getFloat();

  /**
   * Reads a <code>int</code> from the buffer.
   *
   * @return the <code>int</code> read
   */
  public abstract int getInt();

  /**
   * Reads a <code>long</code> from the buffer.
   *
   * @return the <code>long</code> read
   */
  public abstract long getLong();

  /**
   * Reads a <code>short</code> from the buffer.
   *
   * @return the <code>short</code> read
   */
  public abstract short getShort();

  /**
   * @return <code>ByteOrder</code> of the underlying buffer
   */
  public ByteOrder order() {
    return mBuf.order();
  }

  /**
   * @param newByteOrder <code>ByteOrder</code> to use
   */
  public void order(ByteOrder newByteOrder) {
    mBuf.order(newByteOrder);
  }

  /**
   * @return the buffer position
   */
  public abstract int position();

  /**
   * @param newPosition the position to use
   */
  public abstract void position(int newPosition);
}
