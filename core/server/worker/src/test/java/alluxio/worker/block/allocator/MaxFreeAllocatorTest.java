/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.allocator;

import alluxio.Constants;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.reviewer.MockReviewer;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link MaxFreeAllocator}.
 */
public final class MaxFreeAllocatorTest extends AllocatorTestBase {
  @Before
  public void initialize() {
    ServerConfiguration.set(PropertyKey.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    mAllocator = Allocator.Factory.create(getMetadataEvictorView());
  }

  @After
  public void reset() {
    ServerConfiguration.reset();
  }

  /**
   * Tests that blocks are allocated in the storage directory with the most available free space.
   */
  @Test
  public void allocateBlock() throws Exception {
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 2000
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 1500, true, Constants.MEDIUM_SSD, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 500   <--- alloc
    //  1      └───── 2000
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 2000, true, Constants.MEDIUM_SSD, 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0    1000
    //  0      ├───── 500
    //  1      └───── 0   <--- alloc
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 300, true, Constants.MEDIUM_MEM, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700   <--- alloc
    //  0      ├───── 500
    //  1      └───── 0
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc2, 300, true, Constants.MEDIUM_SSD, 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700
    //  0      ├───── 200   <--- alloc
    //  1      └───── 0
    //  0               ├─── 3000
    //  1               ├─── 3000
    //  2               └─── 3000
    //

    /** Reviewer's opinion affects the test */
    MockReviewer.resetBytesToReject(Sets.newHashSet(700L, 2700L, 3000L));
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 300, true, "HDD", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700
    //  0      ├───── 200
    //  1      └───── 0
    //  0               ├─── 2700   <--- alloc
    //  1               ├─── 3000
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyTierLoc, 300, true, "HDD", 1);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700
    //  0      ├───── 200
    //  1      └───── 0
    //  0               ├─── 2700
    //  1               ├─── 2700  <--- alloc
    //  2               └─── 3000
    //
    assertTempBlockMeta(mAllocator, mAnyDirInTierLoc1, 300, false, "", 0);
    //
    // idx | tier1 | tier2 | tier3
    //  0     700
    //  0      ├───── 200
    //  1      └───── 0
    //  0               ├─── 2700
    //  1               ├─── 2700
    //  2               └─── 3000
    //
  }

  @Test
  public void testAnyDirInTier() throws Exception {
    assertAllocationAnyDirInTier();
  }

  @Test
  public void testAnyDirInAnyTierWithMedium() throws Exception {
    assertAllocationAnyDirInAnyTierWithMedium();
  }

  @Test
  public void testSpecificDir() throws Exception {
    assertAllocationInSpecificDir();
  }
}
