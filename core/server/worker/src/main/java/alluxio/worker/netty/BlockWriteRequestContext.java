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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.io.BlockWriter;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The block write request internal representation.
 */
@NotThreadSafe
public final class BlockWriteRequestContext extends WriteRequestContext<BlockWriteRequest> {
  private BlockWriter mBlockWriter;
  private long mBytesReserved;

  BlockWriteRequestContext(Protocol.WriteRequest request, long bytesReserved) {
    super(new BlockWriteRequest(request));
    mBytesReserved = bytesReserved;
  }

  /**
   * @return the block writer
   */
  @Nullable
  public BlockWriter getBlockWriter() {
    return mBlockWriter;
  }

  /**
   * @return the bytes reserved
   */
  public long getBytesReserved() {
    return mBytesReserved;
  }

  /**
   * @param blockWriter block writer to set
   */
  public void setBlockWriter(BlockWriter blockWriter) {
    mBlockWriter = blockWriter;
  }

  /**
   * @param bytesReserved the bytes reserved to set
   */
  public void setBytesReserved(long bytesReserved) {
    mBytesReserved = bytesReserved;
  }
}
