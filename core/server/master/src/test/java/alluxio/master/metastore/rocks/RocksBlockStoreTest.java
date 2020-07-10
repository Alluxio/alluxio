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

package alluxio.master.metastore.rocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.master.metastore.BlockStore;
import alluxio.proto.meta.Block;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Iterator;
import java.util.List;

public class RocksBlockStoreTest {
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Test
  public void testPutGet() throws Exception {
    final int blockCount = 3;
    RocksBlockStore blockStore = new RocksBlockStore(mFolder.newFolder().getAbsolutePath());
    for (int i = 0; i < blockCount; i++) {
      blockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    for (int i = 0; i < blockCount; i++) {
      assertTrue(blockStore.getBlock(i).isPresent());
      assertEquals(i, blockStore.getBlock(i).get().getLength());
    }
  }

  @Test
  public void testIterator() throws Exception {
    final int blockCount = 3;
    RocksBlockStore blockStore = new RocksBlockStore(mFolder.newFolder().getAbsolutePath());
    for (int i = 0; i < blockCount; i++) {
      blockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    Iterator<BlockStore.Block> iter = blockStore.iterator();
    for (int i = 0; i < blockCount; i++) {
      assertTrue(iter.hasNext());
      BlockStore.Block block = iter.next();
      assertEquals(i, block.getId());
      assertEquals(i, block.getMeta().getLength());
    }
    assertFalse(iter.hasNext());
  }

  @Test
  public void blockLocations() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    RocksBlockStore blockStore = new RocksBlockStore(mFolder.newFolder().getAbsolutePath());
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      blockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      blockStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    // validate locations
    for (int i = 0; i < blockCount; i++) {
      List<Block.BlockLocation> locations = blockStore.getLocations(i);
      assertEquals(1, locations.size());
      assertEquals(workerIdStart + i, locations.get(0).getWorkerId());
    }
  }
}
