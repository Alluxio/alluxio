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
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.TieredBlockStoreTestUtils;
import tachyon.worker.block.allocator.Allocator;
import tachyon.worker.block.allocator.MaxFreeAllocator;

/**
 * Test {@link Evictor.Factory} by passing in different evictor strategy class names through the
 * Tachyon configuration and verifying the correct Evictor instance is created.
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
   * {@link tachyon.worker.block.evictor.Evictor.Factory#create(TachyonConf,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createGreedyEvictorTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICTOR_CLASS, GreedyEvictor.class.getName());
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof GreedyEvictor);
  }

  /**
   * Tests that a {@link LRUEvictor} can be created from
   * {@link tachyon.worker.block.evictor.Evictor.Factory#create(TachyonConf,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createLRUEvictorTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICTOR_CLASS, LRUEvictor.class.getName());
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }

  /**
   * Tests that the default evictor can be created from
   * {@link tachyon.worker.block.evictor.Evictor.Factory#create(TachyonConf,
   *        BlockMetadataManagerView, Allocator)}.
   */
  @Test
  public void createDefaultEvictorTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.create(conf, sBlockMetadataManagerView);
    Evictor evictor = Evictor.Factory.create(conf, sBlockMetadataManagerView, allocator);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }
}
