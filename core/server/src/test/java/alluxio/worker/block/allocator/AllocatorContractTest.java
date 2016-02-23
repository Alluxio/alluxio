/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.allocator;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.worker.WorkerContext;

import com.google.common.reflect.ClassPath;
import com.google.common.reflect.Reflection;
import org.junit.Assert;
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
public class AllocatorContractTest extends BaseAllocatorTest {
  protected List<String> mStrategies;

  /**
   *  Try to find all implementation classes of {@link Allocator} in the same package.
   *
   *  @throws Exception if setting up the dependencies fails
   */
  @Before
  @Override
  public void before() throws Exception {
    super.before();
    mStrategies = new ArrayList<String>();
    try {
      String packageName = Reflection.getPackageName(Allocator.class);
      ClassPath path = ClassPath.from(Thread.currentThread().getContextClassLoader());
      List<ClassPath.ClassInfo> clazzInPackage =
          new ArrayList<ClassPath.ClassInfo>(path.getTopLevelClassesRecursive(packageName));
      for (ClassPath.ClassInfo clazz : clazzInPackage) {
        Set<Class<?>> interfaces =
            new HashSet<Class<?>>(Arrays.asList(clazz.load().getInterfaces()));
        if (interfaces.size() > 0 && interfaces.contains(Allocator.class)) {
          mStrategies.add(clazz.getName());
        }
      }
    } catch (Exception e) {
      Assert.fail("Failed to find implementation of allocate strategy");
    }
  }

  /**
   * Tests that no allocation happens when the RAM, SSD and HDD size is more than the default one.
   *
   * @throws Exception if a block cannot be allocated
   */
  @Test
  public void shouldNotAllocateTest() throws Exception {
    Configuration conf = WorkerContext.getConf();
    for (String strategyName : mStrategies) {
      conf.set(Constants.WORKER_ALLOCATOR_CLASS, strategyName);
      resetManagerView();
      Allocator allocator = Allocator.Factory.create(conf, getManagerView());
      assertTempBlockMeta(allocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_SSD_SIZE + 1, true);
    }
  }

  /**
   * Tests that allocation happens when the RAM, SSD and HDD size is lower than the default size.
   *
   * @throws Exception if a block cannot be allocated
   */
  @Test
  public void shouldAllocateTest() throws Exception {
    Configuration conf = WorkerContext.getConf();
    for (String strategyName : mStrategies) {
      conf.set(Constants.WORKER_ALLOCATOR_CLASS, strategyName);
      resetManagerView();
      Allocator tierAllocator = Allocator.Factory.create(conf, getManagerView());
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
      Allocator anyAllocator = Allocator.Factory.create(conf, getManagerView());
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
}
