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

/**
 * Tests {@link RoundRobinPolicy}.
 */
public final class RoundRobinPolicyTest {
  @Test
  public void getWrokerTest() {
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    workerInfoList.add(new BlockWorkerInfo("worker1", Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo("worker2", 2 * (long) Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo("worker3", 3 * (long) Constants.GB, 0));
    RoundRobinPolicy policy = new RoundRobinPolicy(workerInfoList);

    Assert.assertEquals("worker2",
        policy.getWorkerForNextBlock(workerInfoList, 2 * (long) Constants.GB));
    Assert.assertEquals("worker3",
        policy.getWorkerForNextBlock(workerInfoList, 2 * (long) Constants.GB));
  }
}
