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

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.master.block.BlockId;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirEvictorView;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.StorageTierEvictorView;
import alluxio.worker.block.meta.StorageTierView;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Unit tests for {@link BlockMetadataView}.
 */
public final class BlockMetadataViewTest {
  private static final int TEST_TIER_ORDINAL = 0;
  private static final int TEST_DIR = 0;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;

  private BlockMetadataManager mMetaManager;
  private BlockMetadataEvictorView mMetadataView;

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
    mMetadataView = spy(new BlockMetadataEvictorView(mMetaManager,
        new HashSet<Long>(), new HashSet<Long>()));
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#getTierView(String)} method.
   */
  @Test
  public void getTierView() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
      StorageTierView tierView = mMetadataView.getTierView(tierAlias);
      assertEquals(tier.getTierAlias(), tierView.getTierViewAlias());
      assertEquals(tier.getTierOrdinal(), tierView.getTierViewOrdinal());
    }
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#getTierViews()} method.
   */
  @Test
  public void getTierViews() {
    assertEquals(mMetaManager.getTiers().size(), mMetadataView.getTierViews().size());
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#getTierViewsBelow(String)} method.
   */
  @Test
  public void getTierViewsBelow() {
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
      assertEquals(mMetaManager.getTiersBelow(tierAlias).size(),
          mMetadataView.getTierViewsBelow(tierAlias).size());
    }
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#getAvailableBytes(BlockStoreLocation)} method.
   */
  @Test
  public void getAvailableBytes() {
    BlockStoreLocation location;
    // When location represents anyTier
    location = BlockStoreLocation.anyTier();
    assertEquals(mMetaManager.getAvailableBytes(location),
        mMetadataView.getAvailableBytes(location));
    // When location represents one particular tier
    for (StorageTier tier : mMetaManager.getTiers()) {
      String tierAlias = tier.getTierAlias();
      location = BlockStoreLocation.anyDirInTier(tierAlias);
      assertEquals(mMetaManager.getAvailableBytes(location),
          mMetadataView.getAvailableBytes(location));
      for (StorageDir dir : tier.getStorageDirs()) {
        // When location represents one particular dir
        location = dir.toBlockStoreLocation();
        assertEquals(mMetaManager.getAvailableBytes(location),
            mMetadataView.getAvailableBytes(location));
      }
    }
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataEvictorView#getBlockMeta(long)}
   * method when the block does not exist.
   */
  @Test
  public void getBlockMetaNotExisting() throws BlockDoesNotExistException {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mMetadataView.getBlockMeta(TEST_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataEvictorView#getTierView(String)}
   * method when the tier alias does not exist.
   */
  @Test
  public void getTierNotExisting() {
    String badTierAlias = Constants.MEDIUM_HDD;
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TIER_VIEW_ALIAS_NOT_FOUND.getMessage(badTierAlias));
    mMetadataView.getTierView(badTierAlias);
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#getBlockMeta(long)}  method.
   */
  @Test
  public void getBlockMeta() throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getDir(TEST_DIR);

    // Add one block to test dir, expect block meta found
    BlockMeta blockMeta = new DefaultBlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addBlockMeta(blockMeta);
    assertEquals(blockMeta, mMetadataView.getBlockMeta(TEST_BLOCK_ID));
    assertTrue(mMetadataView.isBlockEvictable(TEST_BLOCK_ID));

    // Lock this block, expect null result
    when(mMetadataView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    when(mMetadataView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(true);
    assertNull(mMetadataView.getBlockMeta(TEST_BLOCK_ID));
    assertFalse(mMetadataView.isBlockEvictable(TEST_BLOCK_ID));

    // Pin this block, expect null result
    when(mMetadataView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(true);
    when(mMetadataView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    assertNull(mMetadataView.getBlockMeta(TEST_BLOCK_ID));
    assertFalse(mMetadataView.isBlockEvictable(TEST_BLOCK_ID));

    // No Pin or lock on this block, expect block meta found
    when(mMetadataView.isBlockPinned(TEST_BLOCK_ID)).thenReturn(false);
    when(mMetadataView.isBlockLocked(TEST_BLOCK_ID)).thenReturn(false);
    assertEquals(blockMeta, mMetadataView.getBlockMeta(TEST_BLOCK_ID));
    assertTrue(mMetadataView.isBlockEvictable(TEST_BLOCK_ID));
  }

  /**
   * Tests the {@link BlockMetadataEvictorView#isBlockLocked(long)} and the
   * {@link BlockMetadataEvictorView#isBlockPinned(long)} method.
   */
  @Test
  public void isBlockPinnedOrLocked() {
    long inode = BlockId.getFileId(TEST_BLOCK_ID);

    // With no pinned and locked blocks
    assertFalse(mMetadataView.isBlockLocked(TEST_BLOCK_ID));
    assertFalse(mMetadataView.isBlockPinned(TEST_BLOCK_ID));

    // Pin block by passing its inode to mMetadataView
    HashSet<Long> pinnedInodes = new HashSet<>();
    Collections.addAll(pinnedInodes, inode);
    mMetadataView =
        new BlockMetadataEvictorView(mMetaManager, pinnedInodes, new HashSet<Long>());
    assertFalse(mMetadataView.isBlockLocked(TEST_BLOCK_ID));
    assertTrue(mMetadataView.isBlockPinned(TEST_BLOCK_ID));

    // lock block
    HashSet<Long> testBlockIdSet = new HashSet<>();
    Collections.addAll(testBlockIdSet, TEST_BLOCK_ID);
    Collections.addAll(pinnedInodes, TEST_BLOCK_ID);
    mMetadataView = new BlockMetadataEvictorView(mMetaManager, new HashSet<Long>(),
        testBlockIdSet);
    assertTrue(mMetadataView.isBlockLocked(TEST_BLOCK_ID));
    assertFalse(mMetadataView.isBlockPinned(TEST_BLOCK_ID));

    // Pin and lock block
    mMetadataView = new BlockMetadataEvictorView(mMetaManager, pinnedInodes,
        testBlockIdSet);
    assertTrue(mMetadataView.isBlockLocked(TEST_BLOCK_ID));
    assertTrue(mMetadataView.isBlockPinned(TEST_BLOCK_ID));
  }

  /**
   * Assert if two TierViews are the same by comparing their contents.
   */
  private void assertSameTierView(StorageTierEvictorView tierView1,
      StorageTierEvictorView tierView2) {
    assertEquals(tierView1.getTierViewAlias(), tierView2.getTierViewAlias());
    assertEquals(tierView1.getTierViewOrdinal(), tierView2.getTierViewOrdinal());
    List<StorageDirView> dirViews1 = tierView1.getDirViews();
    List<StorageDirView> dirViews2 = tierView2.getDirViews();
    assertEquals(dirViews1.size(), dirViews2.size());
    for (int i = 0; i < dirViews1.size(); i++) {
      StorageDirEvictorView dirView1 = (StorageDirEvictorView) dirViews1.get(i);
      StorageDirEvictorView dirView2 = (StorageDirEvictorView) dirViews2.get(i);
      assertEquals(dirView1.getAvailableBytes(), dirView2.getAvailableBytes());
      assertEquals(dirView1.getCapacityBytes(), dirView2.getCapacityBytes());
      assertEquals(dirView1.getCommittedBytes(), dirView2.getCommittedBytes());
      assertEquals(dirView1.getDirViewIndex(), dirView2.getDirViewIndex());
      assertEquals(dirView1.getEvictableBlocks(), dirView2.getEvictableBlocks());
      assertEquals(dirView1.getEvitableBytes(), dirView2.getEvitableBytes());
    }
  }

  /**
   * Tests that {@code BlockMetadataEvictorView.getTierView(tierAlias)} returns the same
   * TierView as {@code new StorageTierEvictorView(mMetadataManager.getTier(tierAlias), this)}.
   */
  @Test
  public void sameTierView() {
    String tierAlias = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getTierAlias();
    StorageTierView tierView1 = mMetadataView.getTierView(tierAlias);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getDir(TEST_DIR);
    BlockMeta blockMeta = new DefaultBlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    try {
      dir.addBlockMeta(blockMeta);
    } catch (Exception e) {
      e.printStackTrace();
    }
    StorageTierEvictorView tierView2 =
        new StorageTierEvictorView(mMetaManager.getTier(tierAlias), mMetadataView);
    assertSameTierView((StorageTierEvictorView) tierView1, tierView2);
  }

  /**
   * Tests that {@link BlockMetadataEvictorView#getTierViewsBelow(String)} returns the same
   * TierViews as constructing by {@link BlockMetadataManager#getTiersBelow(String)}.
   */
  @Test
  public void sameTierViewsBelow() {
    String tierAlias = mMetaManager.getTiers().get(TEST_TIER_ORDINAL).getTierAlias();
    List<StorageTierView> tierViews1 = mMetadataView.getTierViewsBelow(tierAlias);

    // Do some operations on metadata
    StorageDir dir = mMetaManager.getTiers().get(TEST_TIER_ORDINAL + 1).getDir(TEST_DIR);
    BlockMeta blockMeta = new DefaultBlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    try {
      dir.addBlockMeta(blockMeta);
    } catch (Exception e) {
      e.printStackTrace();
    }
    List<StorageTier> tiers2 = mMetaManager.getTiersBelow(tierAlias);
    assertEquals(tierViews1.size(), tiers2.size());
    for (int i = 0; i < tierViews1.size(); i++) {
      assertSameTierView((StorageTierEvictorView) tierViews1.get(i),
          new StorageTierEvictorView(tiers2.get(i), mMetadataView));
    }
  }
}
