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

package alluxio.client.block.stream;

import alluxio.client.block.BlockStoreContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.wire.WorkerNetAddress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class LocalBlockOutStream extends BlockOutStream {
  /**
   * Constructs a new {@link PacketOutStream}.
   *
   * @param blockId the id of the block
   * @param blockSize the size of the block
   */
  public LocalBlockOutStream(WorkerNetAddress workerNetAddress, long blockId, long blockSize,
      BlockStoreContext blockStoreContext, OutStreamOptions options) throws IOException {
    super(workerNetAddress, blockId, blockSize, blockStoreContext, options);
  }

  @Override
  protected PacketWriter createPacketWriter() throws IOException {
    return new LocalPacketWriter(mBlockWorkerClient, mId);
  }
}
