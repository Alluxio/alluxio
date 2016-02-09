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

package alluxio.client.file.policy;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link LocalFirstPolicy}.
 */
public final class LocalFirstPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the local host is returned first.
   */
  @Test
  public void getLocalFirst() {
    String localhostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost(localhostName)
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    Assert.assertEquals(localhostName,
        policy.getWorkerForNextBlock(workerInfoList, Constants.MB).getHost());
  }

  /**
   * Tests that another worker is picked in case the local host does not have enough space.
   */
  @Test
  public void getOthersWhenNotEnoughSpaceOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName(ClientContext.getConf());
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workerInfoList = Lists.newArrayList();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost(localhostName)
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.MB, Constants.MB));
    Assert.assertEquals("worker1",
        policy.getWorkerForNextBlock(workerInfoList, Constants.GB).getHost());
  }
}
