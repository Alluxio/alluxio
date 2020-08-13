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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A buffer allocator.
 */
public class BufferAllocator {
  private final long mAllocationLimit;
  private final PooledByteBufAllocator mByteBufAllocator;

  /**
   * Currently allocated amount of memory.
   */
  private final AtomicLong mLocallyHeldMemory = new AtomicLong(0);

  /**
   * Default allocator instance.
   */
  public static final BufferAllocator DEFAULT = new BufferAllocator(-1);

  /**
   * Creates a new instance of {@link BufferAllocator}.
   *
   * @param maxCapacity the maximum amount of memory this allocator is allowed to allocate
   */
  public BufferAllocator(long maxCapacity) {
    if (maxCapacity < 1) {
      mAllocationLimit =
          ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_READER_MAX_BUFFER_SIZE_BYTES);
    } else {
      mAllocationLimit = maxCapacity;
    }
    mByteBufAllocator = PooledByteBufAllocator.DEFAULT;
  }

  /**
   * Allocate a {@link ByteBuf} with the given capacity size.
   * If it is a direct or heap buffer depends on the actual
   * implementation.
   *
   * @param capacitySize the capacity size
   * @return a {@link ByteBuf}
   */
  public ByteBuf buffer(int capacitySize) {
    ByteBuf byteBuf = mByteBufAllocator.buffer(capacitySize, capacitySize);
    mLocallyHeldMemory.getAndAdd(capacitySize);
    return new MutableWrappedByteBuf(byteBuf, mLocallyHeldMemory);
  }

  /**
   * @return Returns the number of bytes of memory used by a {@link ByteBufAllocator}
   */
  public long getAllocatedMemory() {
    return mLocallyHeldMemory.get();
  }

  /**
   * @return Return whether or not this allocator is over its limits
   */
  public boolean isOverLimit() {
    long used = getAllocatedMemory();
    return used > mAllocationLimit;
  }
}
