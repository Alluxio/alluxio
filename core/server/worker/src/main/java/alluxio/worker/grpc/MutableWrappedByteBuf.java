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

package alluxio.worker.grpc;

import io.netty.buffer.AbstractByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.SlicedByteBuf;
import io.netty.util.ByteProcessor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is basically a complete copy of netty's DuplicatedByteBuf.
 * We copy because we want to override
 * some behaviors and make buffer mutable.
 */
public class MutableWrappedByteBuf extends AbstractByteBuf {
  private ByteBuf mBuffer;
  private AtomicLong mLocallyHeldMemory;
  private int mCapacity;

  /**
   * Creates a new instance of {@link BufferAllocator}.
   *
   * @param buffer            a {@link ByteBuf}
   * @param locallyHeldMemory a {@link AtomicLong}
   */
  public MutableWrappedByteBuf(ByteBuf buffer, AtomicLong locallyHeldMemory) {
    super(buffer.maxCapacity());

    if (buffer instanceof MutableWrappedByteBuf) {
      mBuffer = ((MutableWrappedByteBuf) buffer).mBuffer;
    } else {
      mBuffer = buffer;
    }
    mLocallyHeldMemory = locallyHeldMemory;
    mCapacity = mBuffer.capacity();
    setIndex(buffer.readerIndex(), buffer.writerIndex());
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return unwrap().nioBuffer(index, length);
  }

  @Override
  public ByteBuf unwrap() {
    return mBuffer;
  }

  @Override
  public ByteBufAllocator alloc() {
    return mBuffer.alloc();
  }

  @Override
  public ByteOrder order() {
    return mBuffer.order();
  }

  @Override
  public boolean isDirect() {
    return mBuffer.isDirect();
  }

  @Override
  public int capacity() {
    return mBuffer.capacity();
  }

  @Override
  public ByteBuf capacity(int newCapacity) {
    if (mCapacity != newCapacity) {
      throw new UnsupportedOperationException();
    }
    return this;
  }

  @Override
  public boolean hasArray() {
    return mBuffer.hasArray();
  }

  @Override
  public byte[] array() {
    return mBuffer.array();
  }

  @Override
  public int arrayOffset() {
    return mBuffer.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return mBuffer.hasMemoryAddress();
  }

  @Override
  public long memoryAddress() {
    return mBuffer.memoryAddress();
  }

  @Override
  public byte getByte(int index) {
    return _getByte(index);
  }

  @Override
  protected byte _getByte(int index) {
    return mBuffer.getByte(index);
  }

  @Override
  public short getShort(int index) {
    return _getShort(index);
  }

  @Override
  protected short _getShort(int index) {
    return mBuffer.getShort(index);
  }

  @Override
  public short getShortLE(int index) {
    return mBuffer.getShortLE(index);
  }

  @Override
  protected short _getShortLE(int index) {
    return mBuffer.getShortLE(index);
  }

  @Override
  public int getUnsignedMedium(int index) {
    return _getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return mBuffer.getUnsignedMedium(index);
  }

  @Override
  public int getUnsignedMediumLE(int index) {
    return mBuffer.getUnsignedMediumLE(index);
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    return mBuffer.getUnsignedMediumLE(index);
  }

  @Override
  public int getInt(int index) {
    return _getInt(index);
  }

  @Override
  protected int _getInt(int index) {
    return mBuffer.getInt(index);
  }

  @Override
  public int getIntLE(int index) {
    return mBuffer.getIntLE(index);
  }

  @Override
  protected int _getIntLE(int index) {
    return mBuffer.getIntLE(index);
  }

  @Override
  public long getLong(int index) {
    return _getLong(index);
  }

  @Override
  protected long _getLong(int index) {
    return mBuffer.getLong(index);
  }

  @Override
  public long getLongLE(int index) {
    return mBuffer.getLongLE(index);
  }

  @Override
  protected long _getLongLE(int index) {
    return mBuffer.getLongLE(index);
  }

  @Override
  public ByteBuf copy(int index, int length) {
    return new MutableWrappedByteBuf(mBuffer.copy(index, length), mLocallyHeldMemory);
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return new SlicedByteBuf(this, index, length);
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    mBuffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    mBuffer.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    mBuffer.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length)
      throws IOException {
    mBuffer.getBytes(index, out, length);
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length)
      throws IOException {
    return mBuffer.getBytes(index, out, length);
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length)
      throws IOException {
    return mBuffer.getBytes(index, out, position, length);
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    _setByte(index, value);
    return this;
  }

  @Override
  protected void _setByte(int index, int value) {
    mBuffer.setByte(index, value);
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    _setShort(index, value);
    return this;
  }

  @Override
  protected void _setShort(int index, int value) {
    mBuffer.setShort(index, value);
  }

  @Override
  public ByteBuf setShortLE(int index, int value) {
    mBuffer.setShortLE(index, value);
    return this;
  }

  @Override
  protected void _setShortLE(int index, int value) {
    mBuffer.setShortLE(index, value);
  }

  @Override
  public ByteBuf setMedium(int index, int value) {
    _setMedium(index, value);
    return this;
  }

  @Override
  protected void _setMedium(int index, int value) {
    mBuffer.setMedium(index, value);
  }

  @Override
  public ByteBuf setMediumLE(int index, int value) {
    mBuffer.setMediumLE(index, value);
    return this;
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    mBuffer.setMediumLE(index, value);
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    _setInt(index, value);
    return this;
  }

  @Override
  protected void _setInt(int index, int value) {
    mBuffer.setInt(index, value);
  }

  @Override
  public ByteBuf setIntLE(int index, int value) {
    mBuffer.setIntLE(index, value);
    return this;
  }

  @Override
  protected void _setIntLE(int index, int value) {
    mBuffer.setIntLE(index, value);
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    _setLong(index, value);
    return this;
  }

  @Override
  protected void _setLong(int index, long value) {
    mBuffer.setLong(index, value);
  }

  @Override
  public ByteBuf setLongLE(int index, long value) {
    mBuffer.setLongLE(index, value);
    return this;
  }

  @Override
  protected void _setLongLE(int index, long value) {
    mBuffer.setLongLE(index, value);
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    mBuffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    mBuffer.setBytes(index, src, srcIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    mBuffer.setBytes(index, src);
    return this;
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length)
      throws IOException {
    return mBuffer.setBytes(index, in, position, length);
  }

  @Override
  public int setBytes(int index, InputStream in, int length)
      throws IOException {
    return mBuffer.setBytes(index, in, length);
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length)
      throws IOException {
    return mBuffer.setBytes(index, in, length);
  }

  @Override
  public int nioBufferCount() {
    return mBuffer.nioBufferCount();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return mBuffer.nioBuffers(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return nioBuffer(index, length);
  }

  @Override
  public int forEachByte(int index, int length, ByteProcessor processor) {
    return mBuffer.forEachByte(index, length, processor);
  }

  @Override
  public int forEachByteDesc(int index, int length, ByteProcessor processor) {
    return mBuffer.forEachByteDesc(index, length, processor);
  }

  @Override
  public final int refCnt() {
    return unwrap().refCnt();
  }

  @Override
  public final ByteBuf touch() {
    unwrap().touch();
    return this;
  }

  @Override
  public final ByteBuf touch(Object hint) {
    unwrap().touch(hint);
    return this;
  }

  @Override
  public final ByteBuf retain() {
    unwrap().retain();
    return this;
  }

  @Override
  public final ByteBuf retain(int increment) {
    unwrap().retain(increment);
    return this;
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    boolean released = handleRelease(unwrap().release(decrement));
    return released;
  }

  private boolean handleRelease(boolean result) {
    if (result) {
      deallocate();
    }
    return result;
  }

  /**
   * Called once {@link #refCnt()} is equals 0.
   */
  private void deallocate() {
    int size = capacity();
    mLocallyHeldMemory.addAndGet(-size);
  }
}
