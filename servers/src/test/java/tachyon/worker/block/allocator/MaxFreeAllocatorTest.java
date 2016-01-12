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

import org.junit.Test;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Unit tests for {@link MaxFreeAllocator}.
 */
public class MaxFreeAllocatorTest extends BaseAllocatorTest {

  /**
   * Tests that block are allocated in the storage directory with the most available free space.
   *
   * @throws Exception if adding the metadata of the block fails
   */
  @Test
  public void allocateBlockTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    mAllocator = Allocator.Factory.create(conf, getManagerView());
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 2000
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1500, true, "SSD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 500   <--- alloc
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 2000, true, "SSD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 500
    //  1      └───── 0   <--- alloc
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 300, true, "MEM", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700   <--- alloc
    //  0      ├───── 500
    //  1      └───── 0
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 300, true, "SSD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700
    //  0      ├───── 200   <--- alloc
    //  1      └───── 0
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
  }
}
