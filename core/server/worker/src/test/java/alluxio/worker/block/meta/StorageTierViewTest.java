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

package alluxio.worker.block.meta;

import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;

/**
 * Unit tests for {@link StorageTierView}.
 */
public class StorageTierViewTest {
  private static final int TEST_TIER_LEVEL = 0;
  private StorageTier mTestTier;
  private StorageTierEvictorView mTestTierView;
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
    BlockMetadataManager metaManager =
        TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mMetadataView =
        new BlockMetadataEvictorView(metaManager, new HashSet<Long>(),
            new HashSet<Long>());
    mTestTier = metaManager.getTiers().get(TEST_TIER_LEVEL);
    mTestTierView = new StorageTierEvictorView(mTestTier, mMetadataView);
  }

  /**
   * Tests the {@link StorageTierEvictorView#getDirViews()} method.
   */
  @Test
  public void getDirViews() {
    Assert.assertEquals(TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length, mTestTierView
        .getDirViews().size());
  }

  /**
   * Tests the {@link StorageTierEvictorView#getDirView(int)} method.
   */
  @Test
  public void getDirView() {
    for (int i = 0; i < TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length; i++) {
      Assert.assertEquals(i, mTestTierView.getDirView(i).getDirViewIndex());
    }
  }

  /**
   * Tests that an exception is thrown when trying to get a storage directory view with a bad index.
   */
  @Test
  public void getDirViewBadIndex() {
    int badDirIndex = TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length;
    Assert.assertNull(mTestTierView.getDirView(badDirIndex));
  }

  /**
   * Tests the {@link StorageTierEvictorView#getTierViewAlias()} method.
   */
  @Test
  public void getTierViewAlias() {
    Assert.assertEquals(mTestTier.getTierAlias(), mTestTierView.getTierViewAlias());
  }

  /**
   * Tests the {@link StorageTierEvictorView#getTierViewOrdinal()} method.
   */
  @Test
  public void getTierViewOrdinal() {
    Assert.assertEquals(mTestTier.getTierOrdinal(), mTestTierView.getTierViewOrdinal());
  }

  /**
   * Tests the {@link StorageTierEvictorView#getTierViewOrdinal()} method.
   */
  @Test
  public void getBlockMetadataEvictorView() {
    Assert.assertEquals(mMetadataView, mTestTierView.getBlockMetadataEvictorView());
  }
}
