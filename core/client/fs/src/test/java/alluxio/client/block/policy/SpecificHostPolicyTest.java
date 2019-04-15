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

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link SpecificHostPolicy}.
 */
public final class SpecificHostPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the correct worker is returned when using the policy.
   */
  @Test
  public void policy() {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker2");
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workerInfoList).setBlockInfo(new BlockInfo().setLength(Constants.MB));
    Assert.assertEquals("worker2",
        policy.getWorker(options).getHost());
  }

  /**
   * Tests that no worker is chosen when the worker specified in the policy is not part of the
   * worker list.
   */
  @Test
  public void noMatchingHost() {
    SpecificHostPolicy policy = new SpecificHostPolicy("worker3");
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1F")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    GetWorkerOptions options = GetWorkerOptions.defaults().setBlockWorkerInfos(workerInfoList)
        .setBlockInfo(new BlockInfo().setLength(2 * (long) Constants.GB));
    Assert.assertNull(policy.getWorker(options));
  }
}
