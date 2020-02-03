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

import static org.junit.Assert.fail;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.Reflection;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is the class to test the "contract" of different kinds of allocators,
 * i.e., the general properties the allocators need to follow.
 */
public final class AllocatorContractTest extends AllocatorTestBase {
  protected List<String> mStrategies;

  /**
   *  Try to find all implementation classes of {@link Allocator} in the same package.
   */
  @Before
  @Override
  public void before() throws Exception {
    super.before();
    mStrategies = new ArrayList<>();
    try {
      String packageName = Reflection.getPackageName(Allocator.class);
      ClassPath path = ClassPath.from(Thread.currentThread().getContextClassLoader());
      List<ClassPath.ClassInfo> clazzInPackage =
          new ArrayList<>(path.getTopLevelClassesRecursive(packageName));
      for (ClassPath.ClassInfo clazz : clazzInPackage) {
        Set<Class<?>> interfaces =
            new HashSet<>(Arrays.asList(clazz.load().getInterfaces()));
        if (interfaces.size() > 0 && interfaces.contains(Allocator.class)) {
          mStrategies.add(clazz.getName());
        }
      }
    } catch (Exception e) {
      fail("Failed to find implementation of allocate strategy");
    }
  }

  /**
   * Tests that no allocation happens when the RAM, SSD and HDD size is more than the default one.
   */
  @Test
  public void shouldNotAllocate() throws Exception {
    for (String strategyName : mStrategies) {
      ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, strategyName);
      resetManagerView();
      Allocator allocator = Allocator.Factory.create(getMetadataEvictorView());
      assertTempBlockMeta(allocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_SSD_SIZE + 1, true);
    }
  }

  /**
   * Tests that allocation happens when the RAM, SSD and HDD size is lower than the default size.
   */
  @Test
  public void shouldAllocate() throws Exception {
    for (String strategyName : mStrategies) {
      ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, strategyName);
      resetManagerView();
      Allocator tierAllocator = Allocator.Factory.create(getMetadataEvictorView());
      for (int i = 0; i < DEFAULT_RAM_NUM; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_SSD_NUM; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_HDD_NUM; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE - 1, true);
      }

      resetManagerView();
      Allocator anyAllocator = Allocator.Factory.create(getMetadataEvictorView());
      for (int i = 0; i < DEFAULT_RAM_NUM; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_RAM_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_SSD_NUM; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_SSD_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_HDD_NUM; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_HDD_SIZE - 1, true);
      }
    }
  }

  @Test
  public void allocateAfterDirDeletion() throws Exception {
    for (String strategyName : mStrategies) {
      ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, strategyName);
      resetManagerView();
      for (int i = 0; i < TIER_ALIAS.length; i++) {
        StorageTier tier = mManager.getTier(TIER_ALIAS[i]);
        tier.removeStorageDir(tier.getDir(0));
      }
      Allocator tierAllocator = Allocator.Factory.create(getMetadataEvictorView());
      for (int i = 0; i < DEFAULT_RAM_NUM - 1; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_SSD_NUM - 1; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_HDD_NUM - 1; i++) {
        assertTempBlockMeta(tierAllocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE - 1, true);
      }

      resetManagerView();
      Allocator anyAllocator = Allocator.Factory.create(getMetadataEvictorView());
      for (int i = 0; i < DEFAULT_RAM_NUM - 1; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_RAM_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_SSD_NUM - 1; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_SSD_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_HDD_NUM - 1; i++) {
        assertTempBlockMeta(anyAllocator, mAnyTierLoc, DEFAULT_HDD_SIZE - 1, true);
      }
    }
  }
}
