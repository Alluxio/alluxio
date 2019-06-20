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

<<<<<<< HEAD
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.PropertyKey;
import alluxio.worker.block.BlockMetadataManagerView;
||||||| parent of ec9f9ceb90... Reduce the information allocator need in createBlockMeta
import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataManagerView;
=======
import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.BlockMetadataEvictorView;
import alluxio.worker.block.BlockMetadataView;
>>>>>>> ec9f9ceb90... Reduce the information allocator need in createBlockMeta
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
public final class AllocatorFactoryTest {
  private BlockMetadataEvictorView mMetadataView;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    String baseDir = mTestFolder.newFolder().getAbsolutePath();
    mMetadataView = TieredBlockStoreTestUtils.defaultMetadataManagerView(baseDir);
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the creation of the {@link GreedyAllocator} via the
   * {@link Allocator.Factory#create(BlockMetadataView)} method.
   */
  @Test
  public void createGreedyAllocator() {
<<<<<<< HEAD
    Configuration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    Assert.assertTrue(allocator instanceof GreedyAllocator);
||||||| parent of ec9f9ceb90... Reduce the information allocator need in createBlockMeta
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    assertTrue(allocator instanceof GreedyAllocator);
=======
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mMetadataView);
    assertTrue(allocator instanceof GreedyAllocator);
>>>>>>> ec9f9ceb90... Reduce the information allocator need in createBlockMeta
  }

  /**
   * Tests the creation of the {@link MaxFreeAllocator} via the
   * {@link Allocator.Factory#create(BlockMetadataView)} method.
   */
  @Test
  public void createMaxFreeAllocator() {
<<<<<<< HEAD
    Configuration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
||||||| parent of ec9f9ceb90... Reduce the information allocator need in createBlockMeta
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    assertTrue(allocator instanceof MaxFreeAllocator);
=======
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mMetadataView);
    assertTrue(allocator instanceof MaxFreeAllocator);
>>>>>>> ec9f9ceb90... Reduce the information allocator need in createBlockMeta
  }

  /**
   * Tests the creation of the {@link RoundRobinAllocator} via the
   * {@link Allocator.Factory#create(BlockMetadataView)} method.
   */
  @Test
  public void createRoundRobinAllocator() {
<<<<<<< HEAD
    Configuration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    Assert.assertTrue(allocator instanceof RoundRobinAllocator);
||||||| parent of ec9f9ceb90... Reduce the information allocator need in createBlockMeta
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
        RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mManagerView);
    assertTrue(allocator instanceof RoundRobinAllocator);
=======
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS,
        RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mMetadataView);
    assertTrue(allocator instanceof RoundRobinAllocator);
>>>>>>> ec9f9ceb90... Reduce the information allocator need in createBlockMeta
  }

  /**
   * Tests the creation of the default allocator via the
   * {@link Allocator.Factory#create(BlockMetadataView)} method.
   */
  @Test
  public void createDefaultAllocator() {
    // Create a new instance of Alluxio configuration with original properties to test the default
    // behavior of create.
<<<<<<< HEAD
    Allocator allocator = Allocator.Factory.create(mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
||||||| parent of ec9f9ceb90... Reduce the information allocator need in createBlockMeta
    Allocator allocator = Allocator.Factory.create(mManagerView);
    assertTrue(allocator instanceof MaxFreeAllocator);
=======
    Allocator allocator = Allocator.Factory.create(mMetadataView);
    assertTrue(allocator instanceof MaxFreeAllocator);
>>>>>>> ec9f9ceb90... Reduce the information allocator need in createBlockMeta
  }
}
