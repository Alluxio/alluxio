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

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.worker.AlluxioWorker;
import alluxio.worker.DataServer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Unit tests for {@link SpaceReserver}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataServer.Factory.class})
public class SpaceReserverTest {
  private static final long SESSION_ID = 1;
  private static final long BLOCK_SIZE = 100;

  private static final int[] TIER_ORDINAL = {0, 1};
  private static final String[] TIER_ALIAS = {"MEM", "HDD"};
  private static final String[][] TIER_PATH = {{"/ramdisk"}, {"/disk1"}};
  private static final long[][] TIER_CAPACITY_BYTES = {{400}, {1000}};

  private BlockStore mBlockStore;
  private SpaceReserver mSpaceReserver;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  /**
   * Stops the {@link SpaceReserver} and resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    mSpaceReserver.stop();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    String baseDir = mTempFolder.newFolder().getAbsolutePath();
    TieredBlockStoreTestUtils.setupConfWithMultiTier(baseDir, TIER_ORDINAL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, null);

    // Mock away data server creation which would otherwise happen in BlockWorker construction.
    // We shouldn't need to bind net addresses in unit tests
    PowerMockito.mockStatic(DataServer.Factory.class);
    PowerMockito.when(
        DataServer.Factory.create(Mockito.<InetSocketAddress>any(), Mockito.<AlluxioWorker>any()))
        .thenReturn(Mockito.mock(DataServer.class));

    BlockWorker blockWorker = new BlockWorker();
    mBlockStore = blockWorker.getBlockStore();
    String reserveRatioProp =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 0);
    Configuration.set(reserveRatioProp, "0.2");
    reserveRatioProp =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 1);
    Configuration.set(reserveRatioProp, "0.3");
    mSpaceReserver = new SpaceReserver(blockWorker);
  }

  /**
   * Tests that the reserver works as expected.
   */
  @Test
  public void reserveTest() throws Exception {
    // Reserve on top tier
    long blockId = 100;
    BlockStoreLocation tier0 = BlockStoreLocation.anyDirInTier("MEM");
    for (int i = 0; i < 4; i++) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId++, BLOCK_SIZE, mBlockStore, tier0);
    }
    BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
    Map<String, Long> usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(4 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(4 * BLOCK_SIZE, (long) usedBytesOnTiers.get("MEM"));
    Assert.assertEquals(0, (long) usedBytesOnTiers.get("HDD"));

    // Reserver kicks in, expect evicting one block from MEM to HDD
    Whitebox.invokeMethod(mSpaceReserver, "reserveSpace");

    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(4 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(3 * BLOCK_SIZE, (long) usedBytesOnTiers.get("MEM"));
    Assert.assertEquals(1 * BLOCK_SIZE, (long) usedBytesOnTiers.get("HDD"));

    // Reserve on under tier
    for (int i = 0; i < 10; i++) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId++, BLOCK_SIZE, mBlockStore, tier0);
    }
    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(14 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(4 * BLOCK_SIZE, (long) usedBytesOnTiers.get("MEM"));
    Assert.assertEquals(10 * BLOCK_SIZE, (long) usedBytesOnTiers.get("HDD"));

    // Reserver kicks in again, expect evicting one block from MEM to HDD and four blocks from HDD
    Whitebox.invokeMethod(mSpaceReserver, "reserveSpace");

    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(10 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(3 * BLOCK_SIZE, (long) usedBytesOnTiers.get("MEM"));
    Assert.assertEquals(7 * BLOCK_SIZE, (long) usedBytesOnTiers.get("HDD"));
  }
}
