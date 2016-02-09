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

package alluxio.worker.block.meta;

import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Unit tests for {@link StorageTierView}.
 */
public class StorageTierViewTest {
  private static final int TEST_TIER_LEVEL = 0;
  private StorageTier mTestTier;
  private StorageTierView mTestTierView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up a dependency fails
   */
  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    BlockMetadataManager metaManager =
        TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    BlockMetadataManagerView metaManagerView =
        new BlockMetadataManagerView(metaManager, Sets.<Long>newHashSet(),
            Sets.<Long>newHashSet());
    mTestTier = metaManager.getTiers().get(TEST_TIER_LEVEL);
    mTestTierView = new StorageTierView(mTestTier, metaManagerView);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link StorageTierView#getDirViews()} method.
   */
  @Test
  public void getDirViewsTest() {
    Assert.assertEquals(TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length, mTestTierView
        .getDirViews().size());
  }

  /**
   * Tests the {@link StorageTierView#getDirView(int)} method.
   */
  @Test
  public void getDirViewTest() {
    for (int i = 0; i < TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length; i ++) {
      Assert.assertEquals(i, mTestTierView.getDirView(i).getDirViewIndex());
    }
  }

  /**
   * Tests that an exception is thrown when trying to get a storage directory view with a bad index.
   */
  @Test
  public void getDirViewBadIndexTest() {
    mThrown.expect(IndexOutOfBoundsException.class);
    int badDirIndex = TieredBlockStoreTestUtils.TIER_PATH[TEST_TIER_LEVEL].length;
    Assert.assertEquals(badDirIndex, mTestTierView.getDirView(badDirIndex).getDirViewIndex());
  }

  /**
   * Tests the {@link StorageTierView#getTierViewAlias()} method.
   */
  @Test
  public void getTierViewAliasTest() {
    Assert.assertEquals(mTestTier.getTierAlias(), mTestTierView.getTierViewAlias());
  }

  /**
   * Tests the {@link StorageTierView#getTierViewOrdinal()} method.
   */
  @Test
  public void getTierViewOrdinalTest() {
    Assert.assertEquals(mTestTier.getTierOrdinal(), mTestTierView.getTierViewOrdinal());
  }
}
