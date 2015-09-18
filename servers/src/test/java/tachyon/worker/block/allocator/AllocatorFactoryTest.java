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

import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Test {@link Allocator.Factory} by passing different allocate strategy class
 * names with tachyon conf and test if it generates the correct Allocator instance.
 */
public class AllocatorFactoryTest {
  private static TachyonConf sTachyonConf;
  private BlockMetadataManagerView mManagerView;

  @ClassRule
  public static TemporaryFolder sTestFolder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws Exception {
    String baseDir = sTestFolder.newFolder().getAbsolutePath();
    TieredBlockStoreTestUtils.setupTachyonConfDefault(baseDir);
    sTachyonConf = WorkerContext.getConf();
  }

  @Before
  public void before() {
    BlockMetadataManager metaManager = BlockMetadataManager.newBlockMetadataManager();
    mManagerView = new BlockMetadataManagerView(metaManager, Collections.<Long>emptySet(),
        Collections.<Long>emptySet());
  }

  @Test
  public void createGreedyAllocatorTest() {
    sTachyonConf.set(Constants.WORKER_ALLOCATE_STRATEGY_CLASS, GreedyAllocator.class.getName());
    Allocator allocator = Allocator.Factory.createAllocator(sTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof GreedyAllocator);
  }

  @Test
  public void createMaxFreeAllocatorTest() {
    sTachyonConf.set(Constants.WORKER_ALLOCATE_STRATEGY_CLASS, MaxFreeAllocator.class.getName());
    Allocator allocator = Allocator.Factory.createAllocator(sTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }

  @Test
  public void createRoundRobinAllocatorTest() {
    sTachyonConf.set(Constants.WORKER_ALLOCATE_STRATEGY_CLASS, RoundRobinAllocator.class.getName());
    Allocator allocator = Allocator.Factory.createAllocator(sTachyonConf, mManagerView);
    Assert.assertTrue(allocator instanceof RoundRobinAllocator);
  }

  @Test
  public void createDefaultAllocatorTest() {
    /*
     * create a new instance of TachyonConf with original
     * properties to test the default behavior of createAllocator
     */
    TachyonConf conf = new TachyonConf();
    Allocator allocator = Allocator.Factory.createAllocator(conf, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }
}
