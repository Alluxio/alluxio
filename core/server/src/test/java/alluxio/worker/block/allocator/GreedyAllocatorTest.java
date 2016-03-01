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

import org.junit.Test;

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
    Configuration conf = new Configuration();
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
