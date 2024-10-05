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

import alluxio.network.protocol.databuffer.managed.BufOwner;
import alluxio.network.protocol.databuffer.managed.BufferEnvelope;
import alluxio.network.protocol.databuffer.managed.OwnedByteBuf;
import alluxio.network.protocol.databuffer.managed.TrackingOwnedByteBuf;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pooled direct NIO byte buffer allocated from {@link NioDirectBufferPool}
 * wrapped in Netty ByteBuf.
 */
public class PooledDirectNioByteBuf extends RefCountedNioByteBuf {
  private PooledDirectNioByteBuf(ByteBuffer buffer, int length) {
    super(buffer, length, length);
  }

  @Override
  protected void deallocate() {
    NioDirectBufferPool.release(mDelegate);
  }

  /**
   * Allocates a new buffer from {@link NioDirectBufferPool}.
   * Then written buffer's reader and writer indices are both 0.
   *
   * @param length buffer capacity
   * @return the allocated buffer
   */
  public static ByteBuf allocate(int length) {
    return new PooledDirectNioByteBuf(NioDirectBufferPool.acquire(length), length);
  }

  /**
   * Allocates a new ownership managed buffer from {@link NioDirectBufferPool}.
   *
   * @param length buffer capacity
   * @return the allocated buffer in envelope
   */
  public static BufferEnvelope allocateManaged(int length) {
    return new Envelope(
        new PooledDirectNioByteBuf(NioDirectBufferPool.acquire(length), length));
  }

  private static class Envelope implements BufferEnvelope {
    private final AtomicReference<ByteBuf> mBufRef;

    protected Envelope(ByteBuf buffer) {
      mBufRef = new AtomicReference<>(buffer);
    }

    // todo(bowen): make the tracking optional and configurable
    @Override
    public <OwnerT extends BufOwner<OwnerT>> OwnedByteBuf<OwnerT> unseal(OwnerT owner) {
      return TrackingOwnedByteBuf.fromFreshAllocation(mBufRef.getAndSet(null), owner);
    }
  }
}
