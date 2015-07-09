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

package tachyon.worker.block.evictor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.io.Files;

import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This is a parameterized unit test for all types of {@link Evictor} defined in {@link EvictorType}
 *
 * It tests general evictor behavior like not evicting any space when space request is already
 * available, evicting enough space when eviction is needed, having a null eviction plan when the
 * requested space can not be satisfied anyway and so on.
 *
 * Behavior for a specific type of evictor will be tested in other classes, e.x. tests to ensure
 * that blocks evicted by LRUEvictor are in the right order should be in LRUEvictorTest.
 */
@RunWith(Parameterized.class)
public class EvictorTest {
  private static final int USER_ID = 2;
  private static final long BLOCK_ID = 10;
  private BlockMetadataManager mMeta;
  private EvictorType mEvictorType;
  private Evictor mEvictor;
  private File mTempFolder;

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // Run this test against all types of Evictors
    List<Object[]> list = new ArrayList<Object[]>();
    for (EvictorType type : EvictorType.values()) {
      if (type != EvictorType.DEFAULT) {
        list.add(new Object[] {type});
      }
    }
    return list;
  }

  public EvictorTest(EvictorType evictorType) {
    mEvictorType = evictorType;
  }

  @Before
  public final void before() throws IOException {
    mTempFolder = Files.createTempDir();
    mMeta = EvictorTestUtils.defaultMetadataManager(mTempFolder.getAbsolutePath());
    mEvictor = EvictorFactory.create(mEvictorType, mMeta);
  }

  @After
  public final void after() throws IOException {
    FileUtils.forceDelete(mTempFolder);
  }

  @Test
  public void noNeedToEvictTest1() throws IOException {
    // metadata manager is just created, no cached block in Evictor
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        Assert.assertTrue(mEvictor.freeSpace(dir.getCapacityBytes(),
            EvictorTestUtils.location(tier, dir)).isEmpty());
      }
    }
  }

  @Test
  public void noNeedToEvictTest2() throws IOException {
    // cache some data in a dir, then request the remaining space from the dir
    StorageDir dir = EvictorTestUtils.randomDir(mMeta);
    long capacity = dir.getCapacityBytes();
    long cachedBytes = capacity / 2 + 1;
    EvictorTestUtils.cache(USER_ID, BLOCK_ID, cachedBytes, dir, mMeta, mEvictor);
    Assert.assertTrue(mEvictor.freeSpace(capacity - cachedBytes, EvictorTestUtils.location(dir))
        .isEmpty());
  }

  @Test
  public void noNeedToEvictTest3() throws IOException {
    // fill in all dirs except for a random directory in a random tier, then request the capacity of
    // the directory with anyDirInTier
    StorageDir dirLeft = EvictorTestUtils.randomDir(mMeta);
    long blockId = BLOCK_ID; // start from BLOCK_ID
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir != dirLeft) {
          EvictorTestUtils.cache(USER_ID, blockId, dir.getCapacityBytes(), dir, mMeta, mEvictor);
          blockId ++;
        }
      }
    }
    Assert.assertTrue(mEvictor.freeSpace(dirLeft.getCapacityBytes(),
        BlockStoreLocation.anyDirInTier(dirLeft.getParentTier().getTierAlias())).isEmpty());
  }

  @Test
  public void needToEvictTest() throws IOException {
    // Requested space can only be satisfied after evicting some cached blocks

    EvictorTestUtils.randomCache(USER_ID, BLOCK_ID, mMeta, mEvictor);

    // find a non-empty dir
    StorageDir nonEmptyDir = null;
    boolean found = false;
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir.getAvailableBytes() != dir.getCapacityBytes()) {
          nonEmptyDir = dir;
          found = true;
          break;
        }
      }
      if (found) {
        break;
      }
    }

    if (nonEmptyDir == null) {
      // seems like randomCache did not cache any data, so cache data in a random dir
      nonEmptyDir = EvictorTestUtils.randomDir(mMeta);
      EvictorTestUtils.cache(USER_ID, BLOCK_ID, nonEmptyDir.getCapacityBytes() - 1, nonEmptyDir,
          mMeta, mEvictor);
    }

    long requestBytes = nonEmptyDir.getCapacityBytes();
    EvictionPlan plan = mEvictor.freeSpace(requestBytes, EvictorTestUtils.location(nonEmptyDir));
    EvictorTestUtils.assertLegalPlan(requestBytes, plan, mMeta);
  }

  @Test
  public void needToEvictAnyDirInTierTest() throws IOException {
    // cache data with size of "(capacity - 1)" in each dir in a tier, request size of "capacity" of
    // a dir in this tier from anyDirInTier(tier)
    StorageTier tier = mMeta.getTiers().get(0);
    long blockId = BLOCK_ID;
    List<StorageDir> dirs = tier.getStorageDirs();
    for (StorageDir dir : dirs) {
      EvictorTestUtils.cache(USER_ID, blockId, dir.getCapacityBytes() - 1, dir, mMeta, mEvictor);
      blockId ++;
    }

    long requestBytes = dirs.get(0).getCapacityBytes();
    EvictionPlan plan =
        mEvictor.freeSpace(requestBytes, BlockStoreLocation.anyDirInTier(tier.getTierAlias()));
    EvictorTestUtils.assertLegalPlan(requestBytes, plan, mMeta);
  }

  @Test
  public void needToEvictAnyTierTest() throws IOException {
    // cache data with size of "(capacity - 1)" in each dir in all tiers, request size of minimum
    // "capacity" of all dirs from anyTier
    long minCapacity = Long.MAX_VALUE;
    long blockId = BLOCK_ID;
    for (StorageTier tier : mMeta.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        long capacity = dir.getCapacityBytes();
        minCapacity = Math.min(minCapacity, capacity);
        EvictorTestUtils.cache(USER_ID, blockId, capacity - 1, dir, mMeta, mEvictor);
        blockId ++;
      }
    }

    EvictionPlan plan = mEvictor.freeSpace(minCapacity, BlockStoreLocation.anyTier());
    EvictorTestUtils.assertLegalPlan(minCapacity, plan, mMeta);
  }

  @Test
  public void requestSpaceLargerThanCapacityTest() throws IOException {
    long totalCapacity = mMeta.getAvailableBytes(BlockStoreLocation.anyTier());
    StorageDir dir = EvictorTestUtils.randomDir(mMeta);
    BlockStoreLocation dirLocation = EvictorTestUtils.location(dir);
    long dirCapacity = mMeta.getAvailableBytes(dirLocation);

    EvictorTestUtils.randomCache(USER_ID, BLOCK_ID, mMeta, mEvictor);

    // request space larger than total capacity
    Assert.assertNull(mEvictor.freeSpace(totalCapacity + 1, BlockStoreLocation.anyTier()));
    // request space larger than capacity for the random directory
    Assert.assertNull(mEvictor.freeSpace(dirCapacity + 1, dirLocation));
  }
}
