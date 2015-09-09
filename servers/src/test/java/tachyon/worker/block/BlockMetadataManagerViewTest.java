/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.io.File;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import tachyon.exception.ExceptionMessage;
import tachyon.exception.NotFoundException;
import tachyon.master.BlockInfo;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.StorageTierView;

public final class BlockMetadataManagerViewTest {
  private static final int TEST_TIER_LEVEL = 0;
  private static final int TEST_DIR = 0;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;

  private BlockMetadataManager mMetaManager;
  private BlockMetadataManagerView mMetaManagerView;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mMetaManagerView = Mockito.spy(new BlockMetadataManagerView(mMetaManager,
        Sets.<Integer>newHashSet(), Sets.<Long>newHashSet()));
  }

  @Test
  public void getTierViewTest() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      int tierLevel = tier.getTierLevel();
      StorageTierView tierView = mMetaManagerView.getTierView(tierLevel);
      Assert.assertEquals(tier.getTierAlias(), tierView.getTierViewAlias());
      Assert.assertEquals(tier.getTierLevel(), tierView.getTierViewLevel());
    }
  }

  @Test
  public void getTierViewsTest() {
    Assert.assertEquals(mMetaManager.getTiers().size(), mMetaManagerView.getTierViews().size());
  }

  @Test
  public void getTierViewsBelowTest() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      int tierLevel = tier.getTierLevel();
      Assert.assertEquals(mMetaManager.getTiersBelow(tierLevel).size(),
          mMetaManagerView.getTierViewsBelow(tierLevel).size());
    }
  }

  @Test
  public void getAvailableBytesTest() {
    BlockStoreLocation location;
    // When location represents anyTier
    location = BlockStoreLocation.anyTier();
    Assert.assertEquals(mMetaManager.getAvailableBytes(location),
        mMetaManagerView.getAvailableBytes(location));
    // When location represents one particular tier
    for (StorageTier tier : mMetaManager.getTiers()) {
      int tierAlias = tier.getTierAlias().getValue();
      location = BlockStoreLocation.anyDirInTier(tierAlias);
      Assert.assertEquals(mMetaManager.getAvailableBytes(location),
          mMetaManagerView.getAvailableBytes(location));
      for (StorageDir dir : tier.getStorageDirs()) {
        // When location represents one particular dir
        location = dir.toBlockStoreLocation();
        Assert.assertEquals(mMetaManager.getAvailableBytes(location),
            mMetaManagerView.getAvailableBytes(location));
      }
    }
  }

  @Test
  public void getBlockMetaNotExistingTest() throws NotFoundException {
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mMetaManagerView.getBlockMeta(TEST_BLOCK_ID);
  }


  @Test
  public void getTierNotExistingTest() throws Exception {
    int badTierAlias = 3;
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(badTierAlias));
    mMetaManagerView.getTierView(badTierAlias);
  }

  @Test
  public void getBlockMetaTest() throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_LEVEL).getDir(TEST_DIR);

    // Add one block to test dir, expect block meta found
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addBlockMeta(blockMeta);
    Assert.assertEquals(blockMeta, mMetaManagerView.getBlockMeta(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockEvictable(TEST_BLOCK_ID));

    // Lock this block, expect null result
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(true);
    Assert.assertNull(mMetaManagerView.getBlockMeta(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockEvictable(TEST_BLOCK_ID));

    // Pin this block, expect null result
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(true);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    Assert.assertNull(mMetaManagerView.getBlockMeta(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockEvictable(TEST_BLOCK_ID));

    // No Pin or lock on this block, expect block meta found
    Mockito.when(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    Mockito.when(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    Assert.assertEquals(blockMeta, mMetaManagerView.getBlockMeta(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockEvictable(TEST_BLOCK_ID));
  }

  @Test
  public void isBlockPinnedOrLockedTest() {
    int inode = BlockInfo.computeInodeId(TEST_BLOCK_ID);

    // With no pinned and locked blocks
    Assert.assertFalse(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // Pin block by passing its inode to mMetaManagerView
    mMetaManagerView =
        new BlockMetadataManagerView(mMetaManager, Sets.newHashSet(inode), Sets.<Long>newHashSet());
    Assert.assertFalse(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // lock block
    mMetaManagerView = new BlockMetadataManagerView(mMetaManager, Sets.<Integer>newHashSet(),
        Sets.<Long>newHashSet(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // Pin and lock block
    mMetaManagerView = new BlockMetadataManagerView(mMetaManager, Sets.newHashSet(inode),
        Sets.<Long>newHashSet(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));
  }

  /**
   * Assert if two TierViews are the same by comparing their contents.
   */
  private void assertSameTierView(StorageTierView tierView1, StorageTierView tierView2) {
    Assert.assertEquals(tierView1.getTierViewAlias(), tierView2.getTierViewAlias());
    Assert.assertEquals(tierView1.getTierViewLevel(), tierView2.getTierViewLevel());
    List<StorageDirView> dirViews1 = tierView1.getDirViews();
    List<StorageDirView> dirViews2 = tierView2.getDirViews();
    Assert.assertEquals(dirViews1.size(), dirViews2.size());
    for (int i = 0; i < dirViews1.size(); i ++) {
      StorageDirView dirView1 = dirViews1.get(i);
      StorageDirView dirView2 = dirViews2.get(i);
      Assert.assertEquals(dirView1.getAvailableBytes(), dirView2.getAvailableBytes());
      Assert.assertEquals(dirView1.getCapacityBytes(), dirView2.getCapacityBytes());
      Assert.assertEquals(dirView1.getCommittedBytes(), dirView2.getCommittedBytes());
      Assert.assertEquals(dirView1.getDirViewIndex(), dirView2.getDirViewIndex());
      Assert.assertEquals(dirView1.getEvictableBlocks(), dirView2.getEvictableBlocks());
      Assert.assertEquals(dirView1.getEvitableBytes(), dirView2.getEvitableBytes());
    }
  }

  /**
   * Test that <code>BlockMetadataManagerView.getTierView(tierAlias)</code> returns the same
   * TierView as <code>new StorageTierView(mMetadataManager.getTier(tierAlias), this)</code>.
   */
  @Test
  public void sameTierViewTest() throws Exception {
    int tierLevel = mMetaManager.getTiers().get(TEST_TIER_LEVEL).getTierLevel();
    StorageTierView tierView1 = mMetaManagerView.getTierView(tierLevel);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_LEVEL).getDir(TEST_DIR);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addBlockMeta(blockMeta);

    StorageTierView tierView2 =
        new StorageTierView(mMetaManager.getTier(tierLevel), mMetaManagerView);
    assertSameTierView(tierView1, tierView2);
  }

  /**
   * Test that <code>BlockMetadataManagerView.getTierViewsBelow(tierAlias)</code> returns the same
   * TierViews as constructing by <code>BlockMetadataManager.getTiersBelow(tierAlias)</code>.
   */
  @Test
  public void sameTierViewsBelowTest() throws Exception {
    int tierLevel = mMetaManager.getTiers().get(TEST_TIER_LEVEL).getTierLevel();
    List<StorageTierView> tierViews1 = mMetaManagerView.getTierViewsBelow(tierLevel);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_LEVEL + 1).getDir(TEST_DIR);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addBlockMeta(blockMeta);

    List<StorageTier> tiers2 = mMetaManager.getTiersBelow(tierLevel);
    Assert.assertEquals(tierViews1.size(), tiers2.size());
    for (int i = 0; i < tierViews1.size(); i ++) {
      assertSameTierView(tierViews1.get(i), new StorageTierView(tiers2.get(i), mMetaManagerView));
    }
  }
}
