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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;

/**
 * Test {@link Evictor.Factory} by passing in different evictor strategy class names through the
 * Alluxio configuration and verifying the correct Evictor instance is created.
 */
public class EvictorFactoryTest {
  private static BlockMetadataManager sBlockMetadataManager;
  private static BlockMetadataEvictorView sBlockMetadataView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    if (sBlockMetadataView == null) {
      if (sBlockMetadataManager == null) {
        sBlockMetadataManager =
            TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
      }
      sBlockMetadataView = new BlockMetadataEvictorView(sBlockMetadataManager,
          Collections.<Long>emptySet(), Collections.<Long>emptySet());
    }
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  /**
   * Tests that a {@link GreedyEvictor} can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(
   *        BlockMetadataEvictorView, Allocator)}.
   */
  @Test
  public void createGreedyEvictor() {
    ServerConfiguration.set(PropertyKey.WORKER_EVICTOR_CLASS, GreedyEvictor.class.getName());
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(sBlockMetadataView);
    Evictor evictor = Evictor.Factory.create(sBlockMetadataView, allocator);
    Assert.assertTrue(evictor instanceof GreedyEvictor);
  }

  /**
   * Tests that a {@link LRUEvictor} can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(
   *        BlockMetadataEvictorView, Allocator)}.
   */
  @Test
  public void createLRUEvictor() {
    ServerConfiguration.set(PropertyKey.WORKER_EVICTOR_CLASS, LRUEvictor.class.getName());
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(sBlockMetadataView);
    Evictor evictor = Evictor.Factory.create(sBlockMetadataView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }

  /**
   * Tests that the default evictor can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(
   *        BlockMetadataEvictorView, Allocator)}.
   */
  @Test
  public void createDefaultEvictor() {
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(sBlockMetadataView);
    Evictor evictor = Evictor.Factory.create(sBlockMetadataView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }
}
