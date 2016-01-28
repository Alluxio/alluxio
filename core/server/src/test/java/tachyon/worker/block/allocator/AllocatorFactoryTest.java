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

package tachyon.worker.block.allocator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Test {@link Allocator.Factory} by passing different allocate strategy class names with tachyon
 * conf and test if it generates the correct {@link Allocator} instance.
 */
public class AllocatorFactoryTest {
  private TachyonConf mTachyonConf;
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
    mTachyonConf = WorkerContext.getConf();
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
   * {@link tachyon.worker.block.allocator.Allocator.Factory#create(TachyonConf,
   *        BlockMetadataManagerView)} method.
   */
  @Test
  public void createGreedyAllocatorTest() {
    mTachyonConf.set(Constants.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof GreedyAllocator);
  }

  /**
   * Tests the creation of the {@link MaxFreeAllocator} via the
   * {@link tachyon.worker.block.allocator.Allocator.Factory#create(TachyonConf,
   *        BlockMetadataManagerView)} method.
   */
  @Test
  public void createMaxFreeAllocatorTest() {
    mTachyonConf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }

  /**
   * Tests the creation of the {@link RoundRobinAllocator} via the
   * {@link tachyon.worker.block.allocator.Allocator.Factory#create(TachyonConf,
   *        BlockMetadataManagerView)} method.
   */
  @Test
  public void createRoundRobinAllocatorTest() {
    mTachyonConf.set(Constants.WORKER_ALLOCATOR_CLASS, RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(mTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof RoundRobinAllocator);
  }

  /**
   * Tests the creation of the default allocator via the
   * {@link tachyon.worker.block.allocator.Allocator.Factory#create(TachyonConf,
   *        BlockMetadataManagerView)} method.
   */
  @Test
  public void createDefaultAllocatorTest() {
    /*
     * create a new instance of TachyonConf with original
     * properties to test the default behavior of create
     */
    TachyonConf conf = new TachyonConf();
    Allocator allocator = Allocator.Factory.create(conf, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }
}
