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

package tachyon.client.file.policy;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.client.block.BlockWorkerInfo;
import tachyon.worker.NetAddress;

/**
 * Tests {@link SpecificHostPolicy}.
 */
public final class SpecificHostPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the correct worker is returned when using the policy.
   */
  @Test
  public void policyTest() {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker2");
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    workerInfoList.add(
        new BlockWorkerInfo(new NetAddress("worker1", PORT, PORT, PORT), Constants.GB, 0));
    workerInfoList.add(
        new BlockWorkerInfo(new NetAddress("worker2", PORT, PORT, PORT), Constants.GB, 0));
    Assert.assertEquals("worker2",
        policy.getWorkerForNextBlock(workerInfoList, Constants.MB).getHost());
  }

  /**
   * Tests that no worker is chosen when the worker specified in the policy is not part of the
   * worker list.
   */
  @Test
  public void noMatchingHostTest() {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker3");
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    workerInfoList.add(
        new BlockWorkerInfo(new NetAddress("worker1", PORT, PORT, PORT), Constants.GB, 0));
    workerInfoList.add(
        new BlockWorkerInfo(new NetAddress("worker2", PORT, PORT, PORT), Constants.GB, 0));
    Assert.assertNull(policy.getWorkerForNextBlock(workerInfoList, Constants.MB));
  }
}
