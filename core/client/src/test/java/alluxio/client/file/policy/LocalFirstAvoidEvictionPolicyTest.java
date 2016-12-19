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

package alluxio.client.file.policy;

import alluxio.CommonTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link LocalFirstAvoidEvictionPolicy}.
 */
public class LocalFirstAvoidEvictionPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the local host is returned first.
   */
  @Test
  public void getLocalFirst() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy();
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
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
    String localhostName = NetworkAddressUtils.getLocalHostName();
    LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy();
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost(localhostName)
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.MB, Constants.MB));
    Assert.assertEquals("worker1",
        policy.getWorkerForNextBlock(workerInfoList, Constants.MB).getHost());
  }

  /**
   * Tests that local host is picked if none of the workers has enough availability.
   */
  @Test
  public void getLocalWhenNoneHasSpace() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    LocalFirstAvoidEvictionPolicy policy = new LocalFirstAvoidEvictionPolicy();
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, Constants.MB));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost(localhostName)
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, Constants.MB));
    Assert.assertEquals(localhostName,
        policy.getWorkerForNextBlock(workerInfoList, Constants.GB).getHost());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonTestUtils.testEquals(LocalFirstAvoidEvictionPolicy.class);
  }

}
