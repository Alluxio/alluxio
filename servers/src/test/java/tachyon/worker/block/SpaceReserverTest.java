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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.client.WorkerBlockMasterClient;
import tachyon.client.WorkerFileSystemMasterClient;
import tachyon.test.Tester;
import tachyon.util.CommonUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerSource;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WorkerFileSystemMasterClient.class, WorkerBlockMasterClient.class})
public class SpaceReserverTest implements Tester<SpaceReserver> {
  private static final long SESSION_ID = 1;
  private static final long BLOCK_SIZE = 100;

  private static final int[] TIER_LEVEL = {0, 1};
  private static final StorageLevelAlias[] TIER_ALIAS =
      {StorageLevelAlias.MEM, StorageLevelAlias.HDD};
  private static final String[][] TIER_PATH = {{"/ramdisk"}, {"/disk1"}};
  private static final long[][] TIER_CAPACITY_BYTES = {{400}, {1000}};

  private BlockStore mBlockStore;
  private SpaceReserver mSpaceReserver;
  private SpaceReserver.PrivateAccess mPrivateAccess;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @After
  public void after() {
    mSpaceReserver.stop();
  }

  @Before
  public void before() throws Exception {
    WorkerFileSystemMasterClient workerFileSystemMasterClient =
        PowerMockito.mock(WorkerFileSystemMasterClient.class);
    WorkerSource workerSource = PowerMockito.mock(WorkerSource.class);
    WorkerBlockMasterClient blockMasterClient = PowerMockito.mock(WorkerBlockMasterClient.class);
    String baseDir = mTempFolder.newFolder().getAbsolutePath();
    TieredBlockStoreTestUtils.setupTachyonConfWithMultiTier(baseDir, TIER_LEVEL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, null);
    mBlockStore = new TieredBlockStore();
    BlockDataManager blockDataManager = new BlockDataManager(workerSource, blockMasterClient,
        workerFileSystemMasterClient, mBlockStore);
    String reserveRatioProp =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 0);
    WorkerContext.getConf().set(reserveRatioProp, "0.2");
    reserveRatioProp =
        String.format(Constants.WORKER_TIERED_STORE_LEVEL_RESERVED_RATIO_FORMAT, 1);
    WorkerContext.getConf().set(reserveRatioProp, "0.3");
    mSpaceReserver = new SpaceReserver(blockDataManager);
    mSpaceReserver.grantAccess(this);
  }

  @Override
  public void receiveAccess(Object access) {
    mPrivateAccess = (SpaceReserver.PrivateAccess) access;
  }

  @Test
  public void reserveTest() throws Exception {
    // Reserve on top tier
    long blockId = 100;
    BlockStoreLocation tier0 = BlockStoreLocation.anyDirInTier(StorageLevelAlias.MEM.getValue());
    for (int i = 0; i < 4; i ++) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId ++, BLOCK_SIZE, mBlockStore, tier0);
    }
    BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
    List<Long> usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(4 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(4 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.MEM.getValue() - 1));
    Assert.assertEquals(0, (long) usedBytesOnTiers.get(StorageLevelAlias.HDD.getValue() - 1));

    // Reserver kicks in, expect evicting one block from MEM to HHD
    mPrivateAccess.reserveSpace();

    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(4 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(3 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.MEM.getValue() - 1));
    Assert.assertEquals(1 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.HDD.getValue() - 1));

    // Reserve on under tier
    for (int i = 0; i < 10; i ++) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId ++, BLOCK_SIZE, mBlockStore, tier0);
    }
    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(14 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(4 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.MEM.getValue() - 1));
    Assert.assertEquals(10 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.HDD.getValue() - 1));

    // Reserver kicks in again, expect evicting one block from MEM to HHD and four blocks from HHD
    mPrivateAccess.reserveSpace();

    storeMeta = mBlockStore.getBlockStoreMeta();
    usedBytesOnTiers = storeMeta.getUsedBytesOnTiers();
    Assert.assertEquals(10 * BLOCK_SIZE, storeMeta.getUsedBytes());
    Assert.assertEquals(3 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.MEM.getValue() - 1));
    Assert.assertEquals(7 * BLOCK_SIZE,
        (long) usedBytesOnTiers.get(StorageLevelAlias.HDD.getValue() - 1));
  }
}
