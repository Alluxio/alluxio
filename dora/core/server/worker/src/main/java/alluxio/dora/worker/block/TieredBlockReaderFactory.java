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

package alluxio.dora.worker.block;

import alluxio.worker.block.BlockReaderFactory;
import alluxio.worker.block.io.BlockReader;
import alluxio.dora.worker.block.io.StoreBlockReader;
import alluxio.worker.block.meta.BlockMeta;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * Factory for tiered block reader.
 */
public class TieredBlockReaderFactory implements BlockReaderFactory {

  @Override
  public BlockReader createBlockReader(long sessionId, BlockMeta blockMeta, long offset)
      throws IOException {
    BlockReader reader = new StoreBlockReader(sessionId, blockMeta);
    ((FileChannel) reader.getChannel()).position(offset);
    return reader;
  }
}
