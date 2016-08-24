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

package alluxio.worker.block;

import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.master.block.BlockId;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierView;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Unit tests for {@link BlockMetadataManagerView}.
 */
public final class BlockMetadataManagerViewTest {
  private static final int TEST_TIER_ORDINAL = 0;
  private static final int TEST_DIR = 0;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;

  private BlockMetadataManager mMetaManager;
  private BlockMetadataManagerView mMetaManagerView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mMetaManagerView = Mockito.spy(new BlockMetadataManagerView(mMetaManager,
        new HashSet<Long>(), new HashSet<Long>()));
  }

  /**
   * Tests the {@link BlockMetadataManagerView#getTierView(String)} method.
   */
  @Test
  public void getTierView() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
      StorageTierView tierView = mMetaManagerView.getTierView(tierAlias);
      Assert.assertEquals(tier.getTierAlias(), tierView.getTierViewAlias());
      Assert.assertEquals(tier.getTierOrdinal(), tierView.getTierViewOrdinal());
    }
  }

  /**
   * Tests the {@link BlockMetadataManagerView#getTierViews()} method.
   */
  @Test
  public void getTierViews() {
    Assert.assertEquals(mMetaManager.getTiers().size(), mMetaManagerView.getTierViews().size());
  }

  /**
   * Tests the {@link BlockMetadataManagerView#getTierViewsBelow(String)} method.
   */
  @Test
  public void getTierViewsBelow() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
      Assert.assertEquals(mMetaManager.getTiersBelow(tierAlias).size(),
          mMetaManagerView.getTierViewsBelow(tierAlias).size());
    }
  }

  /**
   * Tests the {@link BlockMetadataManagerView#getAvailableBytes(BlockStoreLocation)} method.
   */
  @Test
  public void getAvailableBytes() {
    BlockStoreLocation location;
    // When location represents anyTier
    location = BlockStoreLocation.anyTier();
    Assert.assertEquals(mMetaManager.getAvailableBytes(location),
        mMetaManagerView.getAvailableBytes(location));
    // When location represents one particular tier
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
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

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManagerView#getBlockMeta(long)}
   * method when the block does not exist.
   */
  @Test
  public void getBlockMetaNotExisting() throws BlockDoesNotExistException {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mMetaManagerView.getBlockMeta(TEST_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManagerView#getTierView(String)}
   * method when the tier alias does not exist.
   */
  @Test
  public void getTierNotExisting() {
    String badTierAlias = "HDD";
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(badTierAlias));
    mMetaManagerView.getTierView(badTierAlias);
  }

  /**
   * Tests the {@link BlockMetadataManagerView#getBlockMeta(long)}  method.
   */
  @Test
  public void getBlockMeta() throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getDir(TEST_DIR);

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

  /**
   * Tests the {@link BlockMetadataManagerView#isBlockLocked(long)} and the
   * {@link BlockMetadataManagerView#isBlockPinned(long)} method.
   */
  @Test
  public void isBlockPinnedOrLocked() {
    long inode = BlockId.createBlockId(BlockId.getContainerId(TEST_BLOCK_ID),
        BlockId.getMaxSequenceNumber());

    // With no pinned and locked blocks
    Assert.assertFalse(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // Pin block by passing its inode to mMetaManagerView
    HashSet<Long> pinnedInodes = new HashSet<>();
    Collections.addAll(pinnedInodes, inode);
    mMetaManagerView =
        new BlockMetadataManagerView(mMetaManager, pinnedInodes, new HashSet<Long>());
    Assert.assertFalse(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // lock block
    HashSet<Long> testBlockIdSet = new HashSet<>();
    Collections.addAll(testBlockIdSet, TEST_BLOCK_ID);
    Collections.addAll(pinnedInodes, TEST_BLOCK_ID);
    mMetaManagerView = new BlockMetadataManagerView(mMetaManager, new HashSet<Long>(),
        testBlockIdSet);
    Assert.assertTrue(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertFalse(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));

    // Pin and lock block
    mMetaManagerView = new BlockMetadataManagerView(mMetaManager, pinnedInodes,
        testBlockIdSet);
    Assert.assertTrue(mMetaManagerView.isBlockLocked(TEST_BLOCK_ID));
    Assert.assertTrue(mMetaManagerView.isBlockPinned(TEST_BLOCK_ID));
  }

  /**
   * Assert if two TierViews are the same by comparing their contents.
   */
  private void assertSameTierView(StorageTierView tierView1, StorageTierView tierView2) {
    Assert.assertEquals(tierView1.getTierViewAlias(), tierView2.getTierViewAlias());
    Assert.assertEquals(tierView1.getTierViewOrdinal(), tierView2.getTierViewOrdinal());
    List<StorageDirView> dirViews1 = tierView1.getDirViews();
    List<StorageDirView> dirViews2 = tierView2.getDirViews();
    Assert.assertEquals(dirViews1.size(), dirViews2.size());
    for (int i = 0; i < dirViews1.size(); i++) {
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
   * Tests that {@code BlockMetadataManagerView.getTierView(tierAlias)} returns the same
   * TierView as {@code new StorageTierView(mMetadataManager.getTier(tierAlias), this)}.
   */
  @Test
  public void sameTierView() {
    String tierAlias = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getTierAlias();
    StorageTierView tierView1 = mMetaManagerView.getTierView(tierAlias);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getDir(TEST_DIR);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    try {
      dir.addBlockMeta(blockMeta);
    } catch (Exception e) {
      e.printStackTrace();
    }
    StorageTierView tierView2 =
        new StorageTierView(mMetaManager.getTier(tierAlias), mMetaManagerView);
    assertSameTierView(tierView1, tierView2);
  }

  /**
   * Tests that {@link BlockMetadataManagerView#getTierViewsBelow(String)} returns the same
   * TierViews as constructing by {@link BlockMetadataManager#getTiersBelow(String)}.
   */
  @Test
  public void sameTierViewsBelow() {
    String tierAlias = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getTierAlias();
    List<StorageTierView> tierViews1 = mMetaManagerView.getTierViewsBelow(tierAlias);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL + 1).getDir(TEST_DIR);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    try {
      dir.addBlockMeta(blockMeta);
    } catch (Exception e) {
      e.printStackTrace();
    }
    List<StorageTier> tiers2 = mMetaManager.getTiersBelow(tierAlias);
    Assert.assertEquals(tierViews1.size(), tiers2.size());
    for (int i = 0; i < tierViews1.size(); i++) {
      assertSameTierView(tierViews1.get(i), new StorageTierView(tiers2.get(i), mMetaManagerView));
    }
  }
}
