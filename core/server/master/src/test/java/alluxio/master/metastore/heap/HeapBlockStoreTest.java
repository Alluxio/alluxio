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

package alluxio.master.metastore.heap;

import static org.junit.Assert.assertEquals;

import alluxio.proto.meta.Block;

import org.junit.Test;

public class HeapBlockStoreTest {
  @Test
  public void blockSize() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    HeapBlockStore blockStore = new HeapBlockStore();
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      blockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      blockStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    assertEquals(blockCount, blockStore.size());
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      blockStore.removeBlock(i);
    }

    assertEquals(0, blockStore.size());
  }
}
