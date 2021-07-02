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

package alluxio.worker.block.annotator;

import alluxio.StorageTierAssoc;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.evictor.LRUEvictor;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EmulatingBlockIteratorTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private HashMap<Long, StorageDir> mBlockLocation;
  private Long mUserSession = 1L;

  protected BlockMetadataManager mMetaManager;
  protected BlockIterator mBlockIterator;
  protected BlockStoreEventListener mBlockEventListener;
  protected StorageTierAssoc mTierAssoc;

  /**
   * Sets up base class for LRUAnnotator.
   */
  @Before
  public void before() throws Exception {
    // Set an evictor class in order to activate emulation.
    ServerConfiguration.set(PropertyKey.WORKER_EVICTOR_CLASS, LRUEvictor.class.getName());
    // No reserved bytes for precise capacity planning.
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, "false");
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, "false");

    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mBlockIterator = mMetaManager.getBlockIterator();
    mBlockEventListener = mBlockIterator.getListeners().get(0);
    mTierAssoc = mMetaManager.getStorageTierAssoc();
    mBlockLocation = new HashMap<>();
  }

  @Test
  public void testLRUEmulation() throws Exception {
    // Block size that is common denominator to each dir capacity.
    final long blockSize = 1000;
    // Fill all directories and calculate expected block order for each of them as per LRU.
    Map<StorageDir, List<Long>> expectedListPerDir = new HashMap<>();
    long blockId = 0;
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        expectedListPerDir.put(dir, new LinkedList<>());
        while (dir.getAvailableBytes() > 0) {
          createBlock(++blockId, blockSize, dir);
          accessBlock(blockId);
          expectedListPerDir.get(dir).add(blockId);
        }
      }
    }

    // Validate that emulation iterator yields the same order as expected.
    // Iterators not validated at full length as evictor implementations can stop
    // after it has fulfilled the requested available bytes.
    // That's also why emulation can't be tested reliably with 'Reverse' iteration order.
    for (StorageDir dir : expectedListPerDir.keySet()) {
      validateIteratorHeader(
          mBlockIterator.getIterator(dir.toBlockStoreLocation(), BlockOrder.NATURAL),
          expectedListPerDir.get(dir).iterator());
    }
  }

  private void createBlock(long blockId, long blockSize, StorageDir dir) throws Exception {
    TieredBlockStoreTestUtils.cache2(mUserSession++, blockId, blockSize, dir, mMetaManager,
        mBlockIterator);
    mBlockLocation.put(blockId, dir);
  }

  private void accessBlock(long blockId) throws Exception {
    mBlockEventListener.onAccessBlock(mUserSession++, blockId);
    mBlockEventListener.onAccessBlock(mUserSession++, blockId,
        mBlockLocation.get(blockId).toBlockStoreLocation());
  }

  /**
   * Validates one of the iterator contains the other in the beginning.
   */
  private void validateIteratorHeader(Iterator<Long> iterator, Iterator<Long> expected) {
    // Validate initial state is the same.
    Assert.assertFalse(iterator.hasNext() ^ expected.hasNext());
    while (true) {
      // Quit validation if any of them finished.
      if (!iterator.hasNext() || !expected.hasNext()) {
        break;
      }
      // Validate header is still the same.
      Assert.assertEquals(expected.next(), iterator.next());
    }
  }
}
