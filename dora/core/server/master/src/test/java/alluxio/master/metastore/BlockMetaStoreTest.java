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

import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.ConfigurationRule;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.metastore.heap.HeapBlockMetaStore;
import alluxio.master.metastore.rocks.RocksBlockMetaStore;
import alluxio.proto.meta.Block;
import alluxio.resource.CloseableIterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

@RunWith(Parameterized.class)

public class BlockMetaStoreTest {
  private static final String CONF_NAME = "/rocks-block.ini";
  private static String sDir;

  @Parameterized.Parameters
  public static Collection<Supplier<BlockMetaStore>> data() throws Exception {
    sDir = AlluxioTestDirectory.createTemporaryDirectory(
        "block-store-test").getAbsolutePath();
    File confFile = new File(sDir + CONF_NAME);
    writeStringToFile(confFile, ROCKS_CONFIG, (Charset) null);
    return Arrays.asList(
        () -> new RocksBlockMetaStore(sDir),
        HeapBlockMetaStore::new
    );
  }

  @Parameterized.Parameter
  public Supplier<BlockMetaStore> mBlockMetaStoreSupplier;
  public BlockMetaStore mBlockMetaStore;

  @Before
  public void before() {
    mBlockMetaStore = mBlockMetaStoreSupplier.get();
  }

  @After
  public void after() {
    mBlockMetaStore.close();
  }

  @Test
  public void rocksConfigFile() throws Exception {
    assumeTrue(mBlockMetaStore instanceof RocksBlockMetaStore);
    // close the store first because we want to reopen it with the new config
    mBlockMetaStore.close();
    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_BLOCK_CONF_FILE, sDir + CONF_NAME);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      mBlockMetaStore = mBlockMetaStoreSupplier.get();
      testPutGet();
    }
  }

  @Test
  public void rocksInvalidConfigFile() throws Exception {
    assumeTrue(mBlockMetaStore instanceof RocksBlockMetaStore);
    // close the store first because we want to reopen it with the new config
    mBlockMetaStore.close();
    // write an invalid config
    String path = sDir + CONF_NAME + "invalid";
    File confFile = new File(path);
    writeStringToFile(confFile, "Invalid config", (Charset) null);

    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_BLOCK_CONF_FILE, path);
      }
    }, Configuration.modifiableGlobal()).toResource()) {
      RuntimeException exception = assertThrows(RuntimeException.class, this::before);
      assertEquals(RocksDBException.class, exception.getCause().getClass());
    }
  }

  @Test
  public void testPutGet() {
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
  public void testIterator() {
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
  public void blockLocations() {
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
  public void blockSize() {
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

  // RocksDB configuration options used for the unit tests
  private static final String ROCKS_CONFIG = "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  create_missing_column_families=true\n"
      + "  create_if_missing=true\n"
      + "\n"
      + "\n"
      + "[CFOptions \"default\"]\n"
      + "\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"default\"]\n"
      + "\n"
      + "\n"
      + "[CFOptions \"block-meta\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"block-meta\"]\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"block-locations\"]\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"block-locations\"]\n"
      + "  \n";
}
