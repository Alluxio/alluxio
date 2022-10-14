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

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Pooled NIO byte buffer wrapped in Netty ByteBuf.
 */
public class PooledNioByteBuf extends RefCountedNioByteBuf {
  private PooledNioByteBuf(ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  protected void deallocate() {
    NioDirectBufferPool.release(mDelegate);
  }

  /**
   * Allocates a new buffer.
   * @param length buffer capacity
   * @return the allocated buffer
   */
  public static ByteBuf allocate(int length) {
    return new PooledNioByteBuf(NioDirectBufferPool.acquire(length)).clear();
  }
}
