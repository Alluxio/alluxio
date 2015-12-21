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
import tachyon.client.ClientContext;
import tachyon.client.block.BlockWorkerInfo;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Tests {@link LocalFirstPolicy}.
 */
public final class LocalFirstPolicyTest {

  @Test
  public void getLocalFirst() {
    String localhostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workInfoList = Lists.newArrayList();
    workInfoList.add(new BlockWorkerInfo("worker1", Constants.GB, 0));
    workInfoList.add(new BlockWorkerInfo(localhostName, Constants.GB, 0));
    Assert.assertEquals(localhostName, policy.getWorkerForNextBlock(workInfoList, Constants.MB));
  }

  @Test
  public void getOthersWhenNotEnoughSpaceOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workInfoList = Lists.newArrayList();
    workInfoList.add(new BlockWorkerInfo("worker1", Constants.GB, 0));
    workInfoList.add(new BlockWorkerInfo(localhostName, Constants.GB, Constants.GB));
    Assert.assertEquals("worker1", policy.getWorkerForNextBlock(workInfoList, Constants.MB));
  }
}
