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

package alluxio.worker.block.allocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.reviewer.MockReviewer;

import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;

/**
 * Base class for allocator tests.
 */
public class AllocatorTestBase {

  protected static final long SESSION_ID = 1;
  protected int mTestBlockId = 0;

  // Default tier/dir configurations we use for testing
  public static final long DEFAULT_RAM_SIZE = 1000;
  public static final long DEFAULT_SSD_SIZE = 2000;
  public static final long DEFAULT_HDD_SIZE = 3000;

  public static final String[] MEDIA_TYPES =
      {Constants.MEDIUM_MEM, Constants.MEDIUM_SSD, Constants.MEDIUM_HDD};
  public static final int[] TIER_LEVEL = {0, 1, 2};
  public static final String[] TIER_ALIAS =
      {Constants.MEDIUM_MEM, Constants.MEDIUM_SSD, Constants.MEDIUM_HDD};
  public static final String[][] TIER_PATH = {{"/ramdisk"}, {"/ssd1", "/ssd2"},
      {"/disk1", "/disk2", "/disk3"}};
  public static final String[][] TIER_MEDIA_TYPE = {{Constants.MEDIUM_MEM},
      {Constants.MEDIUM_SSD, Constants.MEDIUM_SSD},
      {Constants.MEDIUM_HDD, Constants.MEDIUM_HDD, Constants.MEDIUM_HDD}};
  public static final long[][] TIER_CAPACITY_BYTES = {{DEFAULT_RAM_SIZE},
      {DEFAULT_SSD_SIZE, DEFAULT_SSD_SIZE},
      {DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE}};

  public static final int DEFAULT_RAM_NUM  = TIER_PATH[0].length;
  public static final int DEFAULT_SSD_NUM  = TIER_PATH[1].length;
  public static final int DEFAULT_HDD_NUM  = TIER_PATH[2].length;

  protected BlockMetadataManager mManager = null;
  protected Allocator mAllocator = null;

  protected BlockStoreLocation mAnyTierLoc = BlockStoreLocation.anyTier();
  protected BlockStoreLocation mAnyDirInTierLoc1 =
      BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM);
  protected BlockStoreLocation mAnyDirInTierLoc2 =
      BlockStoreLocation.anyDirInTier(Constants.MEDIUM_SSD);
  protected BlockStoreLocation mAnyDirInTierLoc3 =
      BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD);

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    resetManagerView();
  }

  /**
   * Resets the manager view configured by the parameters in this base class.
   */
  protected void resetManagerView() throws Exception {
    String alluxioHome = mTestFolder.newFolder().getAbsolutePath();
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, "false");
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, "false");
    ServerConfiguration.set(PropertyKey.WORKER_REVIEWER_CLASS,
            "alluxio.worker.block.reviewer.MockReviewer");
    // Reviewer will not reject by default.
    MockReviewer.resetBytesToReject(Sets.newHashSet());
    TieredBlockStoreTestUtils.setupConfWithMultiTier(alluxioHome, TIER_LEVEL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, TIER_MEDIA_TYPE, null);
    mManager = BlockMetadataManager.createBlockMetadataManager();
  }

  /**
   * Given an allocator with the location and blockSize, we assert whether the block can be
   * allocated.
   *
   * @param allocator the allocation manager of Alluxio managed data
   * @param location the location in block store
   * @param blockSize the size of block in bytes
   * @param avail the block should be successfully allocated or not
   */
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      long blockSize, boolean avail) throws IOException {

    mTestBlockId++;
    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location,
                getMetadataEvictorView(), true);
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(SESSION_ID, mTestBlockId, blockSize);

    if (!avail) {
      assertTrue(tempBlockMeta == null);
    } else {
      assertTrue(tempBlockMeta != null);
    }
  }

  /**
   * Given an allocator with the location, blockSize, tierAlias and dirIndex,
   * we assert whether the block can be allocated.
   *
   * @param allocator the allocation manager of Alluxio managed data
   * @param location the location in block store
   * @param blockSize the size of block in bytes
   * @param avail the block should be successfully allocated or not
   * @param tierAlias the block should be allocated at this tier
   * @param dirIndex  the block should be allocated at this dir
   */
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      int blockSize, boolean avail, String tierAlias, int dirIndex) throws Exception {

    mTestBlockId++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location,
                getMetadataEvictorView(), false);
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(SESSION_ID, mTestBlockId, blockSize);

    if (!avail) {
      assertTrue(tempBlockMeta == null);
    } else {
      assertTrue(tempBlockMeta != null);

      StorageDir pDir = tempBlockMeta.getParentDir();
      StorageTier pTier = pDir.getParentTier();

      assertEquals(dirIndex, pDir.getDirIndex());
      assertEquals(tierAlias, pTier.getTierAlias());

      //update the dir meta info
      pDir.addBlockMeta(new DefaultBlockMeta(mTestBlockId, blockSize, pDir));
    }
  }

  protected BlockMetadataEvictorView getMetadataEvictorView() {
    return new BlockMetadataEvictorView(mManager, new HashSet<Long>(), new HashSet<Long>());
  }

  /**
   * For each tier, test if anyDirInTier location gives a dir in the target tier.
   * */
  protected void assertAllocationAnyDirInTier() throws Exception {
    BlockStoreLocation[] locations = new BlockStoreLocation[]{mAnyDirInTierLoc1,
        mAnyDirInTierLoc2, mAnyDirInTierLoc3};
    for (int i = 0; i < locations.length; i++) {
      StorageDirView dirView =
              mAllocator.allocateBlockWithView(AllocatorTestBase.SESSION_ID, 1,
                      locations[i], getMetadataEvictorView(), true);
      assertNotNull(dirView);
      assertEquals(TIER_ALIAS[i], dirView.getParentTierView().getTierViewAlias());
    }
  }

  /**
   * For each medium, test if anyDirInAnyTierWithMedium gives a dir with the target medium.
   * */
  protected void assertAllocationAnyDirInAnyTierWithMedium() throws Exception {
    for (String medium : MEDIA_TYPES) {
      BlockStoreLocation loc = BlockStoreLocation.anyDirInAnyTierWithMedium(medium);
      StorageDirView dirView =
              mAllocator.allocateBlockWithView(AllocatorTestBase.SESSION_ID, 1, loc,
                      getMetadataEvictorView(), true);
      assertNotNull(dirView);
      assertEquals(medium, dirView.getMediumType());
    }
  }

  protected void assertAllocationInSpecificDir() throws Exception {
    for (int i = 0; i < TIER_ALIAS.length; i++) {
      String tier = TIER_ALIAS[i];
      for (int j = 0; j < TIER_PATH[i].length; j++) {
        BlockStoreLocation loc = new BlockStoreLocation(tier, j);
        StorageDirView dirView =
                mAllocator.allocateBlockWithView(AllocatorTestBase.SESSION_ID, 1, loc,
                        getMetadataEvictorView(), true);
        assertNotNull(dirView);
        assertEquals(tier, dirView.getParentTierView().getTierViewAlias());
        assertEquals(j, dirView.getDirViewIndex());
      }
    }
  }
}
