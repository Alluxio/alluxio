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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.metastore.heap.HeapBlockStore;
import alluxio.master.metastore.rocks.RocksBlockStore;
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
public class BlockStoreTest {
  private static final String CONF_NAME = "/rocks-block.ini";
  private static String sDir;

  @Parameterized.Parameters
  public static Collection<Supplier<BlockStore>> data() throws Exception {
    sDir = AlluxioTestDirectory.createTemporaryDirectory(
        "block-store-test").getAbsolutePath();
    File confFile = new File(sDir + CONF_NAME);
    writeStringToFile(confFile, ROCKS_CONFIG, (Charset) null);
    return Arrays.asList(
        () -> new RocksBlockStore(sDir),
        HeapBlockStore::new
    );
  }

  @Parameterized.Parameter
  public Supplier<BlockStore> mBlockStoreSupplier;
  public BlockStore mBlockStore;

  @Before
  public void before() {
    mBlockStore = mBlockStoreSupplier.get();
  }

  @After
  public void after() {
    mBlockStore.close();
  }

  @Test
  public void rocksConfigFile() throws Exception {
    assumeTrue(mBlockStore instanceof RocksBlockStore);
    // close the store first because we want to reopen it with the new config
    mBlockStore.close();
    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_BLOCK_CONF_FILE, sDir + CONF_NAME);
      }
    }, ServerConfiguration.global()).toResource()) {
      mBlockStore = mBlockStoreSupplier.get();
      testPutGet();
    }
  }

  @Test
  public void rocksInvalidConfigFile() throws Exception {
    assumeTrue(mBlockStore instanceof RocksBlockStore);
    // close the store first because we want to reopen it with the new config
    mBlockStore.close();
    // write an invalid config
    String path = sDir + CONF_NAME + "invalid";
    File confFile = new File(path);
    writeStringToFile(confFile, "Invalid config", (Charset) null);

    try (AutoCloseable ignored = new ConfigurationRule(new HashMap<PropertyKey, Object>() {
      {
        put(PropertyKey.ROCKS_BLOCK_CONF_FILE, path);
      }
    }, ServerConfiguration.global()).toResource()) {
      RuntimeException exception = assertThrows(RuntimeException.class, this::before);
      assertEquals(RocksDBException.class, exception.getCause().getClass());
    }
  }

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

    try (CloseableIterator<BlockStore.Block> iter = mBlockStore.getCloseableIterator()) {
      for (int i = 0; i < blockCount; i++) {
        assertTrue(iter.hasNext());
        BlockStore.Block block = iter.next();
        assertEquals(i, block.getId());
        assertEquals(i, block.getMeta().getLength());
      }
      assertFalse(iter.hasNext());
    }
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

  // RocksDB configuration options used for the unit tests
  private static final String ROCKS_CONFIG = "[Version]\n"
      + "  rocksdb_version=7.0.3\n"
      + "  options_file_version=1.1\n"
      + "\n"
      + "[DBOptions]\n"
      + "  max_total_wal_size=0\n"
      + "  WAL_size_limit_MB=0\n"
      + "  allow_concurrent_memtable_write=false\n"
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
      + "  compression=kNoCompression\n"
      + "  prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "  compression_opts={enabled=false;parallel_threads=1;zstd_max_train_bytes=0;"
      + "max_dict_bytes=0;strategy=0;max_dict_buffer_bytes=0;level=32767;window_bits=-14;}\n"
      + "  memtable_prefix_bloom_size_ratio=0.020000\n"
      + "  max_sequential_skip_in_iterations=8\n"
      + "  compaction_style=kCompactionStyleLevel\n"
      + "  memtable_factory={id=HashLinkListRepFactory;logging_threshold=4096;"
      + "log_when_flash=true;huge_page_size=0;threshold=256;bucket_count=50000;}\n"
      + "  bloom_locality=0\n"
      + "  compression_per_level=kNoCompression:kNoCompression:kLZ4Compression:"
      + "kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression\n"
      + "  num_levels=7\n"
      + "  table_factory=BlockBasedTable\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"block-meta\"]\n"
      + "  block_size=4096\n"
      + "  index_type=kHashSearch\n"
      + "  filter_policy=bloomfilter:10:false\n"
      + "  metadata_block_size=4096\n"
      + "  no_block_cache=false\n"
      + "  index_shortening=kShortenSeparators\n"
      + "  whole_key_filtering=true\n"
      + "  data_block_index_type=kDataBlockBinaryAndHash\n"
      + "  data_block_hash_table_util_ratio=0.750000\n"
      + "  hash_index_allow_collision=true\n"
      + "  \n"
      + "\n"
      + "[CFOptions \"block-locations\"]\n"
      + "  memtable_whole_key_filtering=true\n"
      + "  compression=kNoCompression\n"
      + "  prefix_extractor=rocksdb.FixedPrefix.8\n"
      + "  memtable_prefix_bloom_size_ratio=0.020000\n"
      + "  memtable_factory={id=HashSkipListRepFactory;branching_factor=4;"
      + "skiplist_height=4;bucket_count=1000000;}\n"
      + "  compression_per_level=kNoCompression:kNoCompression:kLZ4Compression:"
      + "kLZ4Compression:kLZ4Compression:kLZ4Compression:kLZ4Compression\n"
      + "  num_levels=7\n"
      + "  table_factory=BlockBasedTable\n"
      + "  \n"
      + "[TableOptions/BlockBasedTable \"block-locations\"]\n"
      + "  block_size=4096\n"
      + "  index_type=kHashSearch\n"
      + "  filter_policy=bloomfilter:10:false\n"
      + "  no_block_cache=false\n"
      + "  whole_key_filtering=true\n"
      + "  data_block_index_type=kDataBlockBinaryAndHash\n"
      + "  data_block_hash_table_util_ratio=0.750000\n"
      + "  hash_index_allow_collision=true\n"
      + "  flush_block_policy_factory=FlushBlockBySizePolicyFactory\n"
      + "  \n";
}
