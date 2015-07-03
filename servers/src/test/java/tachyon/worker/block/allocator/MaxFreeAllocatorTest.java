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

public class MaxFreeAllocatorTest extends BaseAllocatorStrategyTest {

  @Test
  public void allocateBlockTest() throws IOException {
    mAllocator = AllocatorFactory.create(AllocatorType.MAX_FREE, mMetaManager);

    // tier 3, dir 2, remain 1000
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 2000, true, 3, 2);
    // tier 3, dir 1, remain 1000
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 2000, true, 3, 1);
    // tier 2, dir 1, remain 1000
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 1000, true, 2, 1);
    // tier 2, dir 0, remain 1000
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 1000, true, 2, 0);
    // tier 3, dir 0, remain 2000
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, 3, 0);
    // tier 3, dir 0, remain 1000
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, 3, 0);
    // tier 3, dir 2, remain 0
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, 3, 2);
    // tier 3, dir 2, remain 0
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1000, true, 3, 1);
  }
}
