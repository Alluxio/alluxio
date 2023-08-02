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

package alluxio.underfs;

import alluxio.resource.ResourcePool;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A simple byte buffer resource pool. Byte buffer can be replaced with netty ByteBuf in the future.
 */
public class ByteBufferResourcePool extends ResourcePool<ByteBuffer> {
  private final int mBufferSize;

  /**
   * Creates an instance.
   * @param maxCapacity the max capacity
   * @param bufferSize the buffer size
   */
  public ByteBufferResourcePool(int maxCapacity, int bufferSize) {
    super(maxCapacity);
    mBufferSize = bufferSize;
  }

  @Override
  public void close() throws IOException {
    // No-op
  }

  @Override
  public void release(ByteBuffer resource) {
    resource.clear();
    super.release(resource);
  }

  @Override
  public ByteBuffer createNewResource() {
    return ByteBuffer.allocate(mBufferSize);
  }
}
