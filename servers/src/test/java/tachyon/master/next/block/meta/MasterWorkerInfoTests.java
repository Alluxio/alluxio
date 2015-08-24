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

package tachyon.master.next.block.meta;

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for MasterWorkerInfo.
 */
public final class MasterWorkerInfoTests {
  private MasterWorkerInfo mInfo;
  private static final List<Long> TOTAL_BYTES_ON_TIERS =
      Lists.newArrayList(Constants.KB * 3L, Constants.KB * 3L);
  private static final List<Long> USED_BYTES_ON_TIERS = Lists.newArrayList(Constants.KB * 1L);
  private static final Set<Long> NEW_BLOCKS = Sets.newHashSet(Constants.KB * 1L, Constants.KB * 1L);

  @Before
  public void before() {
    // register
    mInfo = new MasterWorkerInfo(0, new NetAddress());
    mInfo.register(TOTAL_BYTES_ON_TIERS, USED_BYTES_ON_TIERS, NEW_BLOCKS);
  }

  @Test
  public void registerTest() {
    Assert.assertEquals(NEW_BLOCKS, mInfo.getBlocks());
    Assert.assertEquals(TOTAL_BYTES_ON_TIERS, mInfo.getTotalBytesOnTiers());
    Assert.assertEquals(Constants.KB * 6L, mInfo.getCapacityBytes());
    Assert.assertEquals(USED_BYTES_ON_TIERS, mInfo.getUsedBytesOnTiers());
    Assert.assertEquals(Constants.KB * 1L, mInfo.getUsedBytes());
  }

  @Test
  public void registerAgainTest() {
    Set<Long> newBlocks = Sets.newHashSet(Constants.KB * 2L);
    Set<Long> removedBlocks = mInfo.register(TOTAL_BYTES_ON_TIERS, USED_BYTES_ON_TIERS, newBlocks);
    Assert.assertEquals(NEW_BLOCKS, removedBlocks);
    Assert.assertEquals(newBlocks, mInfo.getBlocks());
  }
}
