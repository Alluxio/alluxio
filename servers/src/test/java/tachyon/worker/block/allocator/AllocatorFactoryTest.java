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

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.evictor.EvictorTestUtils;

/*
 * Test AllocatorFactory by passing different AllocatorType
 * and test if it generate the correct Allocator instance.
 */
public class AllocatorFactoryTest {
  private BlockMetadataManager mMetaManager;
  private BlockMetadataManagerView mManagerView;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = EvictorTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mManagerView = new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
        Collections.<Long>emptySet());
  }

  @Test
  public void createGreedyAllocatorTest() {
    Allocator allocator = AllocatorFactory.create(AllocatorType.GREEDY, mManagerView);
    Assert.assertTrue(allocator instanceof GreedyAllocator);
  }

  @Test
  public void createMaxFreeAllocatorTest() {
    Allocator allocator = AllocatorFactory.create(AllocatorType.MAX_FREE, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }

  @Test
  public void createDefaultAllocatorTypeTest() {
    Allocator allocator = AllocatorFactory.create(AllocatorType.DEFAULT, mManagerView);
    Assert.assertTrue(allocator instanceof MaxFreeAllocator);
  }
}
