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

public class RoundRobinAllocatorTest extends BaseAllocatorTest {
  @Test
  public void allocateBlockTest() throws Exception {
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, RoundRobinAllocator.class.getName());
    mAllocator = Allocator.Factory.createAllocator(conf, mManagerView);
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 2000
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 500, true, 1, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500   <--- alloc
    //  0      ├───── 2000
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 600, true, 2, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 1400   <--- alloc
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 700, true, 2, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 1400
    //  1      └───── 1300   <--- alloc
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 700, true, 2, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 700   <--- alloc
    //  1      └───── 1300
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, 2, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 700
    //  1      └───── 300   <--- alloc
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 700, true, 2, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 0   <--- alloc
    //  1      └───── 300
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 700, true, 3, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    500
    //  0      ├───── 0
    //  1      └───── 300
    //  0               ├─── 2300   <--- alloc
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc1, 200, true, 1, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    300   <--- alloc
    //  0      ├───── 0
    //  1      └───── 300
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc1, 100, true, 1, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200   <--- alloc
    //  0      ├───── 0
    //  1      └───── 300
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc1, 700, false, 0, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 300
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 100, true, 2, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 200   <--- alloc
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 100, true, 2, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100   <--- alloc
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 1500, false, 0, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 2300
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 2000, true, 3, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 2300
    //  1               ├─── 1000   <--- alloc
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 3000, true, 3, 2);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 2300
    //  1               ├─── 1000
    //  2               └─── 0   <--- alloc
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 500, true, 3, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 1800   <--- alloc
    //  1               ├─── 1000
    //  2               └─── 0
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 2000, false, 0, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 1800
    //  1               ├─── 1000
    //  2               └─── 0
    //
    // tier 3, dir 0, remain 0
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc3, 300, true, 3, 1);
    // idx | tier1 | tier2 | tier3
    //  0    200
    //  0      ├───── 0
    //  1      └───── 100
    //  0               ├─── 1800
    //  1               ├─── 700   <--- alloc
    //  2               └─── 0
    //
  }
}
