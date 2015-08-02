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

/*
 * Test EvictorFactory by passing different AllocatorType and test if it generate the correct
 * Evictor instance.
 */
public class EvictorFactoryTest {
  private static BlockMetadataManager sBlockMetadataManager;
  private static BlockMetadataManagerView sBlockMetadataManagerView;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    if (sBlockMetadataManagerView == null) {
      if (sBlockMetadataManager == null) {
        sBlockMetadataManager =
            EvictorTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
      }
      sBlockMetadataManagerView =
          new BlockMetadataManagerView(sBlockMetadataManager, Collections.<Integer>emptySet(),
              Collections.<Long>emptySet());
    }
  }

  @Test
  public void createGreedyAllocatorTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICT_STRATEGY_CLASS, GreedyEvictor.class.getName());
    Evictor evictor = Evictor.Factory.createEvictor(conf, sBlockMetadataManagerView);
    Assert.assertTrue(evictor instanceof GreedyEvictor);
  }

  @Test
  public void createLRUAllocatorTest() {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICT_STRATEGY_CLASS, LRUEvictor.class.getName());
    Evictor evictor = Evictor.Factory.createEvictor(conf, sBlockMetadataManagerView);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }

  @Test
  public void createDefaultAllocatorTest() {
    TachyonConf conf = new TachyonConf();
    Evictor evictor = Evictor.Factory.createEvictor(conf, sBlockMetadataManagerView);
    Assert.assertTrue(evictor instanceof LRUEvictor);
  }
}
