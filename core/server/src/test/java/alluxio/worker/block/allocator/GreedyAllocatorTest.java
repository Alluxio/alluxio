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

package alluxio.worker.block.allocator;

import org.junit.Test;

import alluxio.Constants;
import alluxio.conf.TachyonConf;

/**
 * Unit tests for {@link GreedyAllocator}.
 */
public class GreedyAllocatorTest extends BaseAllocatorTest {

  /**
   * Tests that blocks are allocated in the first storage directory which has enough free space.
   *
   * @throws Exception if adding the metadata of the block fails
   */
  @Test
  public void allocateBlockTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, GreedyAllocator.class.getName());
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
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 500, true, "MEM", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500   <--- alloc
    //  0      ├───── 2000
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 1000, true, "SSD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500
    //  0      ├───── 1000   <--- alloc
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 1500, true, "SSD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500
    //  0      ├───── 1000
    //  1      └───── 500   <--- alloc
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, "SSD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500
    //  0      ├───── 0   <--- alloc
    //  1      └───── 500
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, "HDD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500
    //  0      ├───── 0
    //  1      └───── 500
    //  0               ├─── 2000   <--- alloc
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 2000, true, "HDD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     500
    //  0      ├───── 0
    //  1      └───── 500
    //  0               ├─── 0   <--- alloc
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 500, true, "MEM", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0      0   <--- alloc
    //  0      ├───── 0
    //  1      └───── 500
    //  0               ├─── 0
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 500, true, "SSD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0      0
    //  0      ├───── 0
    //  1      └───── 0   <--- alloc
    //  0               ├─── 0
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 1000, true, "HDD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0      0
    //  0      ├───── 0
    //  1      └───── 500
    //  0               ├─── 0
    //  1               ├─── 2000   <--- alloc
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 700, true, "HDD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0      0
    //  0      ├───── 0
    //  1      └───── 500
    //  0               ├─── 0
    //  1               ├─── 1300   <--- alloc
    //  2               └─── 3000
    //
  }
}
