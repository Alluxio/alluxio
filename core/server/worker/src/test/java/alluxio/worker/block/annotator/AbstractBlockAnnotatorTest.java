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
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.meta.StorageDir;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractBlockAnnotatorTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private HashMap<Long, StorageDir> mBlockLocation;
  private Long mUserSession = 1L;
  private Long mInternalSession = Long.MIN_VALUE;

  protected BlockMetadataManager mMetaManager;
  protected BlockIterator mBlockIterator;
  protected BlockStoreEventListener mBlockEventListener;
  protected StorageTierAssoc mTierAssoc;

  /**
   * Sets up all dependencies before a test runs.
   */
  public void init() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mBlockIterator = mMetaManager.getBlockIterator();
    mBlockEventListener = mBlockIterator.getListeners().get(0);
    mTierAssoc = mMetaManager.getStorageTierAssoc();
    mBlockLocation = new HashMap<>();
  }

  protected void createBlock(long blockId, StorageDir dir) throws Exception {
    TieredBlockStoreTestUtils.cache2(mUserSession++, blockId, 1, dir, mMetaManager, mBlockIterator);
    mBlockLocation.put(blockId, dir);
  }

  protected void moveBlock(long blockId, StorageDir destDir) throws Exception {
    mMetaManager.removeBlockMeta(mMetaManager.getBlockMeta(blockId));
    TieredBlockStoreTestUtils.cache2(mUserSession++, blockId, 1, destDir, mMetaManager,
        (BlockIterator) null);
    mBlockEventListener.onMoveBlockByWorker(mInternalSession++, blockId,
        mBlockLocation.get(blockId).toBlockStoreLocation(), destDir.toBlockStoreLocation());
    mBlockLocation.put(blockId, destDir);
  }

  protected void removeBlock(long blockId) throws Exception {
    mMetaManager.removeBlockMeta(mMetaManager.getBlockMeta(blockId));
    mBlockEventListener.onRemoveBlock(mUserSession++, blockId,
        mBlockLocation.remove(blockId).toBlockStoreLocation());
  }

  protected void accessBlock(long blockId) throws Exception {
    mBlockEventListener.onAccessBlock(mUserSession++, blockId,
        mBlockLocation.get(blockId).toBlockStoreLocation());
  }

  protected StorageDir getDir(int tierIndex, int dirIndex) {
    return mMetaManager.getDir(new BlockStoreLocation(mTierAssoc.getAlias(tierIndex), dirIndex));
  }

  protected void validateIterator(Iterator<Long> iterator, Iterator<Long> expected) {
    while (true) {
      Assert.assertFalse(iterator.hasNext() ^ expected.hasNext());
      if (iterator.hasNext()) {
        Assert.assertEquals(expected.next(), iterator.next());
      } else {
        break;
      }
    }
  }

  // Common tests for {@link EvictionOrderProvider} implementations.

  @Test
  public void testRemovedBlock() throws Exception {
    StorageDir dir = getDir(0, 0);
    createBlock(0, dir);
    List<Long> expectedList = new ArrayList<Long>() {
      {
        add(0L);
      }
    };
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());

    removeBlock(0);
    expectedList.clear();
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());
  }

  @Test
  public void testMovedBlock() throws Exception {
    StorageDir srcDir = getDir(0, 0);
    StorageDir dstDir = getDir(0, 1);
    createBlock(0, srcDir);
    List<Long> expectedList = new ArrayList<Long>() {
      {
        add(0L);
      }
    };
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());

    moveBlock(0, dstDir);
    validateIterator(mBlockIterator.getIterator(BlockStoreLocation.anyTier(), BlockOrder.NATURAL),
        expectedList.iterator());
  }
}
