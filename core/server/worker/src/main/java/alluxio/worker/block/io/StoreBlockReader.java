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

import alluxio.worker.block.meta.BlockMeta;

import java.io.IOException;

/**
 * A local block reader used by block store.
 * It provides integration with the {@link BlockStreamTracker}.
 */
public class StoreBlockReader extends LocalFileBlockReader {
  /** Session Id for the reader. */
  private final long mSessionId;
  /** Block meta for the reader. */
  private final BlockMeta mBlockMeta;

  /**
   * Creates new block reader for block store.
   *
   * @param sessionId session id
   * @param blockMeta block meta
   * @throws IOException
   */
  public StoreBlockReader(long sessionId, BlockMeta blockMeta) throws IOException {
    super(blockMeta.getPath());
    mSessionId = sessionId;
    mBlockMeta = blockMeta;
    if (mSessionId > 0) {
      BlockStreamTracker.readerOpened(this, mBlockMeta.getBlockLocation());
    }
  }

  @Override
  public void close() throws IOException {
    if (mSessionId > 0) {
      BlockStreamTracker.readerClosed(this, mBlockMeta.getBlockLocation());
    }
    super.close();
  }
}
