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

package alluxio.master.metastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.master.metastore.heap.HeapBlockMetaStore;
import alluxio.master.metastore.rocks.RocksBlockMetaStore;
import alluxio.proto.meta.Block;
import alluxio.resource.CloseableIterator;

import com.google.common.io.Files;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class BlockMetaStoreTest {
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][] {
        {new RocksBlockMetaStore(Files.createTempDir().getAbsolutePath())},
        {new HeapBlockMetaStore()}
    });
  }

  @Parameterized.Parameter
  public BlockMetaStore mBlockMetaStore;

  @Test
  public void testPutGet() throws Exception {
    final int blockCount = 3;
    for (int i = 0; i < blockCount; i++) {
      mBlockMetaStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    for (int i = 0; i < blockCount; i++) {
      assertTrue(mBlockMetaStore.getBlock(i).isPresent());
      assertEquals(i, mBlockMetaStore.getBlock(i).get().getLength());
    }
    mBlockMetaStore.clear();
  }

  @Test
  public void testIterator() throws Exception {
    final int blockCount = 3;
    for (int i = 0; i < blockCount; i++) {
      mBlockMetaStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    try (CloseableIterator<BlockMetaStore.Block> iter = mBlockMetaStore.getCloseableIterator()) {
      for (int i = 0; i < blockCount; i++) {
        assertTrue(iter.hasNext());
        BlockMetaStore.Block block = iter.next();
        assertEquals(i, block.getId());
        assertEquals(i, block.getMeta().getLength());
      }
      assertFalse(iter.hasNext());
    }
    mBlockMetaStore.clear();
  }

  @Test
  public void blockLocations() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockMetaStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      mBlockMetaStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    // validate locations
    for (int i = 0; i < blockCount; i++) {
      List<Block.BlockLocation> locations = mBlockMetaStore.getLocations(i);
      assertEquals(1, locations.size());
      assertEquals(workerIdStart + i, locations.get(0).getWorkerId());
    }
    mBlockMetaStore.clear();
  }

  @Test
  public void blockSize() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockMetaStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      mBlockMetaStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    assertEquals(blockCount, mBlockMetaStore.size());
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockMetaStore.removeBlock(i);
    }

    assertEquals(0, mBlockMetaStore.size());
    mBlockMetaStore.clear();
  }
}
