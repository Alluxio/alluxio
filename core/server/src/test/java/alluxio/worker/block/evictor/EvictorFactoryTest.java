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

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;

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
  private static BlockMetadataManagerView sBlockMetadataManagerView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the meta manager, the lock manager or the evictor fails
   */
  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    if (sBlockMetadataManagerView == null) {
      if (sBlockMetadataManager == null) {
        sBlockMetadataManager =
            TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
      }
      sBlockMetadataManagerView = new BlockMetadataManagerView(sBlockMetadataManager,
          Collections.<Long>emptySet(), Collections.<Long>emptySet());
    }
  }

  /**
   * Tests that a {@link GreedyEvictor} can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(Configuration,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createGreedyEvictorTest() {
    Configuration conf = new Configuration();
    conf.set(Constants.WORKER_EVICTOR_CLASS, GreedyEvictor.class.getName());
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof GreedyEvictor);
  }

  /**
   * Tests that a {@link LRUEvictor} can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(Configuration,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createLRUEvictorTest() {
    Configuration conf = new Configuration();
    conf.set(Constants.WORKER_EVICTOR_CLASS, LRUEvictor.class.getName());
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }

  /**
   * Tests that the default evictor can be created from
   * {@link alluxio.worker.block.evictor.Evictor.Factory#create(Configuration,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createDefaultEvictorTest() {
    Configuration conf = new Configuration();
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }
}
