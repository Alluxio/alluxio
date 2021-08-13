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

import alluxio.master.metastore.heap.HeapBlockStore;
import alluxio.master.metastore.rocks.RocksBlockStore;
import alluxio.proto.meta.Block;

import com.google.common.io.Files;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

@RunWith(Parameterized.class)
public class BlockStoreTest {
  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(new Object[][] {
        {new RocksBlockStore(Files.createTempDir().getAbsolutePath())},
        {new HeapBlockStore()}
    });
  }

  @Parameterized.Parameter
  public BlockStore mBlockStore;

  @Test
  public void testPutGet() throws Exception {
    final int blockCount = 3;
    for (int i = 0; i < blockCount; i++) {
      mBlockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    for (int i = 0; i < blockCount; i++) {
      assertTrue(mBlockStore.getBlock(i).isPresent());
      assertEquals(i, mBlockStore.getBlock(i).get().getLength());
    }
    mBlockStore.clear();
  }

  @Test
  public void testIterator() throws Exception {
    final int blockCount = 3;
    for (int i = 0; i < blockCount; i++) {
      mBlockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
    }

    Iterator<BlockStore.Block> iter = mBlockStore.iterator();
    for (int i = 0; i < blockCount; i++) {
      assertTrue(iter.hasNext());
      BlockStore.Block block = iter.next();
      assertEquals(i, block.getId());
      assertEquals(i, block.getMeta().getLength());
    }
    assertFalse(iter.hasNext());
    mBlockStore.clear();
  }

  @Test
  public void blockLocations() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      mBlockStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    // validate locations
    for (int i = 0; i < blockCount; i++) {
      List<Block.BlockLocation> locations = mBlockStore.getLocations(i);
      assertEquals(1, locations.size());
      assertEquals(workerIdStart + i, locations.get(0).getWorkerId());
    }
    mBlockStore.clear();
  }

  @Test
  public void blockSize() throws Exception {
    final int blockCount = 5;
    final int workerIdStart = 100000;
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockStore.putBlock(i, Block.BlockMeta.newBuilder().setLength(i).build());
      mBlockStore
          .addLocation(i, Block.BlockLocation.newBuilder().setWorkerId(workerIdStart + i).build());
    }

    assertEquals(blockCount, mBlockStore.size());
    // create blocks and locations
    for (int i = 0; i < blockCount; i++) {
      mBlockStore.removeBlock(i);
    }

    assertEquals(0, mBlockStore.size());
    mBlockStore.clear();
  }
}
