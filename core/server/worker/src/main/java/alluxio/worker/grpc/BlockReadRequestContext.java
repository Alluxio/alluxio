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

import alluxio.worker.block.io.BlockReader;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context of {@link BlockReadRequest}.
 */
@NotThreadSafe
public final class BlockReadRequestContext extends ReadRequestContext<BlockReadRequest> {
  private BlockReader mBlockReader;

  /**
   * @param request read request in proto
   */
  public BlockReadRequestContext(alluxio.grpc.ReadRequest request) {
    super(new BlockReadRequest(request));
  }

  /**
   * @return block reader
   */
  @Nullable
  public BlockReader getBlockReader() {
    return mBlockReader;
  }

  /**
   * @param blockReader block reader to set
   */
  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }
}
