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

package alluxio.network.protocol.databuffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.AbstractReferenceCountedByteBuf;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * Reference counted NIO byte buffer wrapped in Netty ByteBuf.
 */
abstract class RefCountedNioByteBuf extends AbstractReferenceCountedByteBuf {
  protected final ByteBuffer mDelegate;
  protected int mCapacity;

  protected RefCountedNioByteBuf(ByteBuffer buffer, int capacity) {
    super(capacity);
    Preconditions.checkArgument(buffer.capacity() >= capacity);
    buffer.clear();
    // enforce big endianness
    buffer.order(ByteOrder.BIG_ENDIAN);
    mDelegate = buffer;
    mCapacity = capacity;
  }

  @Override
  protected byte _getByte(int index) {
    return mDelegate.get(index);
  }

  @Override
  protected short _getShort(int index) {
    return mDelegate.getShort(index);
  }

  @Override
  protected short _getShortLE(int index) {
    return Short.reverseBytes(_getShort(index));
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    byte first = _getByte(index);
    byte second = _getByte(index + 1);
    byte third = _getByte(index + 2);
    return (Byte.toUnsignedInt(first) << 16)
        | (Byte.toUnsignedInt(second) << 8)
        | Byte.toUnsignedInt(third);
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    byte first = _getByte(index);
    byte second = _getByte(index + 1);
    byte third = _getByte(index + 2);
    return (Byte.toUnsignedInt(third) << 16)
        | (Byte.toUnsignedInt(second) << 8)
        | Byte.toUnsignedInt(first);
  }

  @Override
  protected int _getInt(int index) {
    return mDelegate.getInt(index);
  }

  @Override
  protected int _getIntLE(int index) {
    return Integer.reverseBytes(_getInt(index));
  }

  @Override
  protected long _getLong(int index) {
    return mDelegate.getLong(index);
  }

  @Override
  protected long _getLongLE(int index) {
    return Long.reverseBytes(_getLong(index));
  }

  @Override
  protected void _setByte(int index, int value) {
    mDelegate.put(index, (byte) value);
  }

  @Override
  protected void _setShort(int index, int value) {
    mDelegate.putShort(index, (short) value);
  }

  @Override
  protected void _setShortLE(int index, int value) {
    mDelegate.putShort(index, Short.reverseBytes((short) value));
  }

  @Override
  protected void _setMedium(int index, int value) {
    byte first = (byte) ((value & 0x00ff0000) >> 16);
    byte second = (byte) ((value & 0x0000ff00) >> 8);
    byte third = (byte) ((value & 0x0000ff00) >> 8);
    _setByte(index, first);
    _setByte(index + 1, second);
    _setByte(index + 2, third);
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    byte first = (byte) ((value & 0x00ff0000) >> 16);
    byte second = (byte) ((value & 0x0000ff00) >> 8);
    byte third = (byte) ((value & 0x0000ff00) >> 8);
    _setByte(index, third);
    _setByte(index + 1, second);
    _setByte(index + 2, first);
  }

  @Override
  protected void _setInt(int index, int value) {
    mDelegate.putInt(index, value);
  }

  @Override
  protected void _setIntLE(int index, int value) {
    mDelegate.putInt(index, Integer.reverseBytes(value));
  }

  @Override
  protected void _setLong(int index, long value) {
    mDelegate.putLong(index, value);
  }

  @Override
  protected void _setLongLE(int index, long value) {
    mDelegate.putLong(index, Long.reverseBytes(value));
  }

  @Override
  public int capacity() {
    return mCapacity;
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    Preconditions.checkArgument(newCapacity >= 0 && newCapacity <= maxCapacity(),
        "invalid new capacity %s, max capacity is %s", newCapacity, maxCapacity());
    mCapacity = newCapacity;
    return this;
  }

  @Override
  public ByteBufAllocator alloc() {
    // is this correct?
    return ByteBufAllocator.DEFAULT;
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN;
  }

  @Override
  public ByteBuf unwrap() {
    return null;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    ensureIndexInBounds(index, capacity(), dstIndex, dst.capacity(), length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return dst.setBytes(dstIndex, dup);
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    ensureIndexInBounds(index, capacity(), dstIndex, dst.length, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    dup.get(dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    ensureIndexInBounds(index, capacity(), dst.position(), dst.capacity(), dst.remaining());
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + dst.remaining());
    dst.put(dup);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    ensureIndexInBounds(index, capacity(), 0, Integer.MAX_VALUE, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    if (dup.hasArray()) {
      byte[] byteArray = dup.array();
      int arrayOffset = dup.arrayOffset();
      out.write(byteArray, arrayOffset + index, length);
    } else {
      // use smaller buffer?
      byte[] copied = new byte[length];
      dup.put(copied);
      out.write(copied);
    }
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    ensureIndexInBounds(index, capacity(), 0, Integer.MAX_VALUE, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return out.write(dup);
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    ensureIndexInBounds(index, capacity(), position, Long.MAX_VALUE, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return out.write(dup, position);
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    ensureIndexInBounds(srcIndex, src.capacity(), index, capacity(), length);
    src.getBytes(srcIndex, this, index, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    ensureIndexInBounds(srcIndex, src.length, index, capacity(), length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    dup.put(src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    ensureIndexInBounds(src.position(), src.limit(), index, capacity(), src.remaining());
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + src.remaining());
    dup.put(src);
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    ensureIndexInBounds(0, Integer.MAX_VALUE, index, capacity(), length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    if (dup.hasArray()) {
      byte[] bufferArray = dup.array();
      int arrayOffset = dup.arrayOffset();
      return in.read(bufferArray, arrayOffset + index, length);
    } else {
      // use smaller buffer?
      byte[] bufferArray = new byte[length];
      int read = in.read(bufferArray);
      dup.put(bufferArray, 0, read);
      return read;
    }
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    ensureIndexInBounds(0, Integer.MAX_VALUE, index, capacity(), length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return in.read(dup);
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    ensureIndexInBounds(position, in.size(), index, capacity(), length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return in.read(dup, position);
  }

  @Override
  public ByteBuf copy(int index, int length) {
    ensureIndexInBounds(index, capacity(), 0, Long.MAX_VALUE, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return Unpooled.copiedBuffer(dup);
  }

  @Override
  public int nioBufferCount() {
    return 1;
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    ensureIndexInBounds(index, capacity(), 0, Integer.MAX_VALUE, length);
    ByteBuffer dup = mDelegate.duplicate();
    dup.position(index);
    dup.limit(index + length);
    return dup;
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return nioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    ByteBuffer[] buffers = new ByteBuffer[1];
    buffers[0] = nioBuffer(index, length);
    return buffers;
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    throw new UnsupportedOperationException("array()");
  }

  @Override
  public int arrayOffset() {
    throw new UnsupportedOperationException("arrayOffset()");
  }

  @Override
  public boolean hasMemoryAddress() {
    return false;
  }

  @Override
  public long memoryAddress() {
    throw new UnsupportedOperationException("memoryAddress()");
  }

  private static void ensureIndexInBounds(
      long index, long srcCapacity, long dstIndex, long dstCapacity, int length) {
    if (srcCapacity < 0 || dstCapacity < 0 || length < 0) {
      throw new IndexOutOfBoundsException(
          String.format("negative capacity or length: srcCapacity %d, dstCapacity %d, length %d",
              srcCapacity, dstCapacity, length));
    }
    if (index < 0 || index > srcCapacity
        || (length > 0 && index == srcCapacity)) // tolerate length == 0 when end of buffer
    {
      throw new IndexOutOfBoundsException(
          String.format("invalid index %d, srcCapacity %d", index, srcCapacity));
    }
    if (index + length < 0 || index + length > srcCapacity) {
      throw new IndexOutOfBoundsException(
          String.format("index %d + length %d exceeds srcCapacity %d", index, length, srcCapacity));
    }
    if (dstIndex < 0 || dstIndex > dstCapacity
        || (length > 0 && dstIndex == dstCapacity))
    {
      throw new IndexOutOfBoundsException(
          String.format("invalid dstIndex %d, dstCapacity %d", dstIndex, dstCapacity));
    }
    if (dstIndex + length < 0 || dstIndex + length > dstCapacity) {
      throw new IndexOutOfBoundsException(
          String.format("dstIndex %d + length %d exceeds dstCapacity %d",
              dstIndex, length, srcCapacity));
    }
  }
}
