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
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test {@link Allocator.Factory} by passing different allocate strategy class names with alluxio
 * conf and test if it generates the correct {@link Allocator} instance.
 */
public class AllocatorFactoryTest {
  private Configuration mConfiguration;
  private BlockMetadataManagerView mManagerView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the dependencies fails
   */
  @Before
  public void before() throws Exception {
    String baseDir = mTestFolder.newFolder().getAbsolutePath();
    mManagerView = TieredBlockStoreTestUtils.defaultMetadataManagerView(baseDir);
    mConfiguration = WorkerContext.getConf();
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the creation of the {@link GreedyAllocator} via the
   * {@link Allocator.Factory#create(Configuration, BlockMetadataManagerView)} method.
   */
  @Test
  public void createGreedyAllocatorTest() {
    mConfiguration.set(Constants.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mConfiguration, mManagerView);
    Assert.assertTrue(allocator instanceof GreedyAllocator);
  }

  /**
   * Tests the creation of the {@link MaxFreeAllocator} via the
   * {@link Allocator.Factory#create(Configuration, BlockMetadataManagerView)} method.
   */
  @Test
  public void createMaxFreeAllocatorTest() {
    mConfiguration.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mConfiguration, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }

  /**
   * Tests the creation of the {@link RoundRobinAllocator} via the
   * {@link Allocator.Factory#create(Configuration, BlockMetadataManagerView)} method.
   */
  @Test
  public void createRoundRobinAllocatorTest() {
    mConfiguration.set(Constants.WORKER_ALLOCATOR_CLASS, RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mConfiguration, mManagerView);
    Assert.assertTrue(allocator instanceof RoundRobinAllocator);
  }

  /**
   * Tests the creation of the default allocator via the
   * {@link Allocator.Factory#create(Configuration, BlockMetadataManagerView)} method.
   */
  @Test
  public void createDefaultAllocatorTest() {
    // Create a new instance of Alluxio configuration with original properties to test the default
    // behavior of create.
    Configuration conf = new Configuration();
    Allocator allocator = Allocator.Factory.create(conf, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }
}
