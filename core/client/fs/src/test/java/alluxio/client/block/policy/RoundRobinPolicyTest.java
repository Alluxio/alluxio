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

package alluxio.client.block.policy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.test.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link RoundRobinPolicy}.
 */
public final class RoundRobinPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the correct workers are chosen when round robin is used.
   */
  @Test
  public void getWorker() {
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), 2 * (long) Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker3")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), 3 * (long) Constants.GB, 0));
    RoundRobinPolicy policy = new RoundRobinPolicy(ConfigurationTestUtils.defaults());

    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(workerInfoList)
        .setBlockInfo(new BlockInfo().setLength(2 * (long) Constants.GB));
    assertNotEquals(
        policy.getWorker(options).getHost(),
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(123))).getHost());

    assertEquals(
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(555))).getHost(),
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(555))).getHost());
  }

  /**
   * Tests that no workers are returned when there are no eligible workers.
   */
  @Test
  public void getWorkerNoneEligible() {
    RoundRobinPolicy policy = new RoundRobinPolicy(ConfigurationTestUtils.defaults());
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(new ArrayList<>())
        .setBlockInfo(new BlockInfo().setLength(2 * (long) Constants.GB));
    assertNull(policy.getWorker(options));
  }

  /**
   * Tests that no workers are returned when subsequent calls to the policy have no eligible
   * workers.
   */
  @Test
  public void getWorkerNoneEligibleAfterCache() {
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));

    RoundRobinPolicy policy = new RoundRobinPolicy(ConfigurationTestUtils.defaults());
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(workerInfoList)
        .setBlockInfo(new BlockInfo().setLength((long) Constants.MB));
    assertNotNull(policy.getWorker(options));
    options.setBlockWorkerInfos(new ArrayList<>());
    assertNull(policy.getWorker(options));
  }

  @Test
  public void equalsTest() throws Exception {
    AlluxioConfiguration conf = ConfigurationTestUtils.defaults();
    CommonUtils.testEquals(RoundRobinPolicy.class, new Class[]{AlluxioConfiguration.class},
        new Object[]{conf});
  }
}
