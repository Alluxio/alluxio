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

package alluxio.worker.block.evictor;

import alluxio.ConfigurationTestUtils;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.Reflection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
public final class EvictorContractTest extends EvictorTestBase {
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
    List<Object[]> list = new ArrayList<>();
    try {
      String packageName = Reflection.getPackageName(Evictor.class);
      ClassPath path = ClassPath.from(Thread.currentThread().getContextClassLoader());
      List<ClassPath.ClassInfo> clazzInPackage =
          new ArrayList<>(path.getTopLevelClassesRecursive(packageName));
      for (ClassPath.ClassInfo clazz : clazzInPackage) {
        Set<Class<?>> interfaces =
            new HashSet<>(Arrays.asList(clazz.load().getInterfaces()));
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
  public EvictorContractTest(String evictorClassName) {
    mEvictorClassName = evictorClassName;
  }

  /**
   * Sets up all dependencies before a test runs.
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
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests that no eviction plan is created whn there is no cached block in the evictor.
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
          blockId++;
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
   */
  @Test
  public void needToEvict() throws Exception {
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
   */
  @Test
  public void needToEvictAnyDirInTier() throws Exception {
    // cache data with size of "(capacity - 1)" in each dir in a tier, request size of "capacity" of
    // the last dir(whose capacity is largest) in this tier from anyDirInTier(tier), all blocks
    // cached in the last dir should be in the eviction plan.
    StorageTier tier = mMetaManager.getTiers().get(0);
    long blockId = BLOCK_ID;
    List<StorageDir> dirs = tier.getStorageDirs();
    for (StorageDir dir : dirs) {
      TieredBlockStoreTestUtils.cache(SESSION_ID, blockId, dir.getCapacityBytes() - 1, dir,
          mMetaManager, mEvictor);
      blockId++;
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
   */
  @Test
  public void needToEvictAnyTier() throws Exception {
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
        blockId++;
      }
    }

    EvictionPlan plan =
        mEvictor.freeSpaceWithView(minCapacity, BlockStoreLocation.anyTier(), mManagerView);
    EvictorTestUtils.assertEvictionPlanValid(minCapacity, plan, mMetaManager);
  }

  /**
   * Tests that no eviction plan is available when requesting more space than capacity available.
   */
  @Test
  public void requestSpaceLargerThanCapacity() throws Exception {
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
