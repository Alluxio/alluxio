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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.Reflection;

import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * This is a parametrized unit test for classes that implement the {@link Evictor} interface.
 *
 * It performs sanity check on evictors regardless of their types, in cases such as not evicting any
 * blocks when the required space is already available, proposed eviction ensuring enough space, and
 * returning null eviction plan when the requests can not be achieved.
 *
 * Behavior for a specific type of evictor is tested in other classes, e.g. tests to ensure that
 * blocks evicted by {@link LRUEvictor} are in the right order should be in {@link LRUEvictorTest}.
 */
@RunWith(Parameterized.class)
public class EvictorContractTestBase extends EvictorTestBase {
  private static final int TEST_TIER_LEVEL = 0;
  private static final int TEST_DIR = 0;

  private StorageDir mTestDir;
  private final String mEvictorClassName;

  /**
   * @return a list of all evictors
   */
  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    // Run this test against all types of Evictors
    List<Object[]> list = new ArrayList<Object[]>();
    try {
      String packageName = Reflection.getPackageName(Evictor.class);
      ClassPath path = ClassPath.from(Thread.currentThread().getContextClassLoader());
      List<ClassPath.ClassInfo> clazzInPackage =
          new ArrayList<ClassPath.ClassInfo>(path.getTopLevelClassesRecursive(packageName));
      for (ClassPath.ClassInfo clazz : clazzInPackage) {
        Set<Class<?>> interfaces =
            new HashSet<Class<?>>(Arrays.asList(clazz.load().getInterfaces()));
        if (!Modifier.isAbstract(clazz.load().getModifiers()) && interfaces.size() > 0
            && interfaces.contains(Evictor.class)) {
          list.add(new Object[] {clazz.getName()});
        }
      }
    } catch (Exception e) {
      Assert.fail("Failed to find implementation of allocate strategy");
    }
    return list;
  }

  /**
   * @param evictorClassName the class name of the evictor
   */
  public EvictorContractTestBase(String evictorClassName) {
    mEvictorClassName = evictorClassName;
  }

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the meta manager, the lock manager or the evictor fails
   */
  @Before
  public final void before() throws Exception {
    init(mEvictorClassName);

    List<StorageTier> tiers = mMetaManager.getTiers();
    mTestDir = tiers.get(TEST_TIER_LEVEL).getDir(TEST_DIR);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests that no eviction plan is created whn there is no cached block in the evictor.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void noNeedToEvictTest1() throws Exception {
    // metadata manager is just created, no cached block in Evictor,
    // so when trying to make sure each dir has free space of its capacity,
    // the eviction plan should be empty.
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        Assert.assertTrue(mEvictor.freeSpaceWithView(dir.getCapacityBytes(),
            dir.toBlockStoreLocation(), mManagerView).isEmpty());
      }
    }
  }

  /**
   * Tests that no eviction plan is created when there is enough space in a directory.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void noNeedToEvictTest2() throws Exception {
    // cache some data in a dir, then request the remaining space from the dir, the eviction plan
    // should be empty.
    StorageDir dir = mTestDir;
    long capacity = dir.getCapacityBytes();
    long cachedBytes = capacity / 2 + 1;
    TieredBlockStoreTestUtils.cache(SESSION_ID, BLOCK_ID, cachedBytes, dir, mMetaManager, mEvictor);
    Assert.assertTrue(mEvictor.freeSpaceWithView(capacity - cachedBytes,
        dir.toBlockStoreLocation(), mManagerView).isEmpty());
  }

  /**
   * Tests that no eviction plan is created when all directories are filled except for one
   * directory.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void noNeedToEvictTest3() throws Exception {
    // fill in all dirs except for one directory, then request the capacity of
    // the directory with anyDirInTier
    StorageDir dirLeft = mTestDir;
    long blockId = BLOCK_ID; // start from BLOCK_ID
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        if (dir != dirLeft) {
          TieredBlockStoreTestUtils.cache(SESSION_ID, blockId, dir.getCapacityBytes(), dir,
              mMetaManager, mEvictor);
          blockId ++;
        }
      }
    }

    Assert.assertTrue(mEvictor.freeSpaceWithView(dirLeft.getCapacityBytes(),
        BlockStoreLocation.anyDirInTier(dirLeft.getParentTier().getTierAlias()), mManagerView)
        .isEmpty());
  }

  /**
   * Tests that an eviction plan is created when a directory is filled and the request size is the
   * capacity of the directory.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void needToEvictTest() throws Exception {
    // fill in a dir and request the capacity of the dir, all cached data in the dir should be
    // evicted.
    StorageDir dir = mTestDir;
    long capacityBytes = dir.getCapacityBytes();
    TieredBlockStoreTestUtils.cache(SESSION_ID, BLOCK_ID, capacityBytes, dir, mMetaManager,
        mEvictor);

    EvictionPlan plan =
        mEvictor.freeSpaceWithView(capacityBytes, dir.toBlockStoreLocation(), mManagerView);
    EvictorTestUtils.assertEvictionPlanValid(capacityBytes, plan, mMetaManager);
  }

  /**
   * Tests that an eviction plan is created when all capacity is used in each directory in a tier
   * and the request size is the capacity of the largest directory.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void needToEvictAnyDirInTierTest() throws Exception {
    // cache data with size of "(capacity - 1)" in each dir in a tier, request size of "capacity" of
    // the last dir(whose capacity is largest) in this tier from anyDirInTier(tier), all blocks
    // cached in the last dir should be in the eviction plan.
    StorageTier tier = mMetaManager.getTiers().get(0);
    long blockId = BLOCK_ID;
    List<StorageDir> dirs = tier.getStorageDirs();
    for (StorageDir dir : dirs) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId, dir.getCapacityBytes() - 1, dir,
          mMetaManager, mEvictor);
      blockId ++;
    }

    long requestBytes = dirs.get(dirs.size() - 1).getCapacityBytes();
    EvictionPlan plan =
        mEvictor.freeSpaceWithView(requestBytes,
            BlockStoreLocation.anyDirInTier(tier.getTierAlias()), mManagerView);
    EvictorTestUtils.assertEvictionPlanValid(requestBytes, plan, mMetaManager);
  }

  /**
   * Tests that an eviction plan is created when all capacity is used in each directory in all tiers
   * and the request size is the minimum capacity of all directories.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void needToEvictAnyTierTest() throws Exception {
    // cache data with size of "(capacity - 1)" in each dir in all tiers, request size of minimum
    // "capacity" of all dirs from anyTier
    long minCapacity = Long.MAX_VALUE;
    long blockId = BLOCK_ID;
    for (StorageTier tier : mMetaManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        long capacity = dir.getCapacityBytes();
        minCapacity = Math.min(minCapacity, capacity);
        TieredBlockStoreTestUtils
            .cache(SESSION_ID, blockId, capacity - 1, dir, mMetaManager, mEvictor);
        blockId ++;
      }
    }

    EvictionPlan plan =
        mEvictor.freeSpaceWithView(minCapacity, BlockStoreLocation.anyTier(), mManagerView);
    EvictorTestUtils.assertEvictionPlanValid(minCapacity, plan, mMetaManager);
  }

  /**
   * Tests that no eviction plan is available when requesting more space than capacity available.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void requestSpaceLargerThanCapacityTest() throws Exception {
    // cache data in a dir
    long totalCapacity = mMetaManager.getAvailableBytes(BlockStoreLocation.anyTier());
    StorageDir dir = mTestDir;
    BlockStoreLocation dirLocation = dir.toBlockStoreLocation();
    long dirCapacity = mMetaManager.getAvailableBytes(dirLocation);

    TieredBlockStoreTestUtils.cache(SESSION_ID, BLOCK_ID, dirCapacity, dir, mMetaManager, mEvictor);

    // request space larger than total capacity, no eviction plan should be available
    Assert.assertNull(mEvictor.freeSpaceWithView(totalCapacity + 1, BlockStoreLocation.anyTier(),
        mManagerView));
    // request space larger than capacity for the random directory, no eviction plan should be
    // available
    Assert.assertNull(mEvictor.freeSpaceWithView(dirCapacity + 1, dirLocation, mManagerView));
  }
}
