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

package alluxio.worker.block.io;

import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;

/**
 * A local block writer used by block store.
 * It provides integration with the {@link BlockStreamTracker}.
 */
public class StoreBlockWriter extends LocalFileBlockWriter {
  /** Temp block meta for the writer. */
  private final TempBlockMeta mBlockMeta;

  /**
   * Creates new block writer for block store.
   *
   * @param blockMeta temp block meta
   * @throws IOException
   */
  public StoreBlockWriter(TempBlockMeta blockMeta) throws IOException {
    super(blockMeta.getPath());
    mBlockMeta = blockMeta;
    if (mBlockMeta.getSessionId() > 0) {
      BlockStreamTracker.writerOpened(this, mBlockMeta.getBlockLocation());
    }
  }

  @Override
  public void close() throws IOException {
    if (mBlockMeta.getSessionId() > 0) {
      BlockStreamTracker.writerClosed(this, mBlockMeta.getBlockLocation());
    }
    super.close();
  }
}
