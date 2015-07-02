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

import java.io.IOException;

import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockMetadataManager;

/**
 * This is the class to test the "contact" of different kinds of allocators,
 * i.e., the general properties the allocators need to follow
 */
public class AllocatorContractTest extends BaseAllocatorTest {
  
  @Test
  public void shouldNotAllocateTest() throws IOException {
    for (AllocatorType type : AllocatorType.values()) {
      TachyonConf tachyonConf = createTestTachyonConf();
      BlockMetadataManager metaManager = BlockMetadataManager.newBlockMetadataManager(tachyonConf);
      Allocator allocator = AllocatorFactory.create(type, metaManager);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_HDD_SIZE + 1, false);
      assertTempBlockMeta(allocator, mAnyTierLoc, DEFAULT_SSD_SIZE + 1, true);
    }
  }
  
  @Test
  public void shouldAllocateTest() throws IOException {
    for (AllocatorType type : AllocatorType.values()) {
      TachyonConf tachyonConf = createTestTachyonConf();
      BlockMetadataManager metaManager = BlockMetadataManager.newBlockMetadataManager(tachyonConf);
      Allocator allocator = AllocatorFactory.create(type, metaManager);
      for (int i = 0; i < DEFAULT_RAM_NUM; i ++) {
        assertTempBlockMeta(allocator, mAnyDirInTierLoc1, DEFAULT_RAM_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_SSD_NUM; i ++) {
        assertTempBlockMeta(allocator, mAnyDirInTierLoc2, DEFAULT_SSD_SIZE - 1, true);
      }
      for (int i = 0; i < DEFAULT_HDD_NUM; i ++) {
        assertTempBlockMeta(allocator, mAnyDirInTierLoc3, DEFAULT_HDD_SIZE - 1, true);
      }
    }
  }
}
