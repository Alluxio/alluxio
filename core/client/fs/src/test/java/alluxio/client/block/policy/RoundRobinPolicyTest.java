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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
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
   * Tests that the correct workers are chosen when round-robin is used.
   */
  @Test
  public void getWorker() {
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(WorkerNetAddress.newBuilder().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT).build(), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(WorkerNetAddress.newBuilder().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT).build(), 2 * (long) Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(WorkerNetAddress.newBuilder().setHost("worker3")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT).build(), 3 * (long) Constants.GB, 0));
    RoundRobinPolicy policy = new RoundRobinPolicy(Configuration.global());

    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(workerInfoList)
        .setBlockInfo(new BlockInfo().setLength(2 * (long) Constants.GB));
    assertNotEquals(
        policy.getWorker(options).orElseThrow(
            () -> new IllegalStateException("Expected worker")).getHost(),
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(123)))
            .orElseThrow(() -> new IllegalStateException("Expected worker")).getHost());

    assertEquals(
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(555)))
            .orElseThrow(() -> new IllegalStateException("Expected worker")).getHost(),
        policy.getWorker(options.setBlockInfo(options.getBlockInfo().setBlockId(555)))
            .orElseThrow(() -> new IllegalStateException("Expected worker")).getHost());
  }

  /**
   * Tests that no workers are returned when there are no eligible workers.
   */
  @Test
  public void getWorkerNoneEligible() {
    RoundRobinPolicy policy = new RoundRobinPolicy(Configuration.global());
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(new ArrayList<>())
        .setBlockInfo(new BlockInfo().setLength(2 * (long) Constants.GB));
    assertFalse(policy.getWorker(options).isPresent());
  }

  /**
   * Tests that no workers are returned when subsequent calls to the policy have no eligible
   * workers.
   */
  @Test
  public void getWorkerNoneEligibleAfterCache() {
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(WorkerNetAddress.newBuilder().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT).build(), Constants.GB, 0));

    RoundRobinPolicy policy = new RoundRobinPolicy(Configuration.global());
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(workerInfoList)
        .setBlockInfo(new BlockInfo().setLength(Constants.MB));
    assertTrue(policy.getWorker(options).isPresent());
    options.setBlockWorkerInfos(new ArrayList<>());
    assertFalse(policy.getWorker(options).isPresent());
  }

  @Test
  public void equalsTest() {
    AlluxioConfiguration conf = Configuration.global();
    CommonUtils.testEquals(RoundRobinPolicy.class, new Class[]{AlluxioConfiguration.class},
        new Object[]{conf});
  }
}
