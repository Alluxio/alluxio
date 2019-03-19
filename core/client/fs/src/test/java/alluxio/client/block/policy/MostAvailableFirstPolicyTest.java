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

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.test.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link MostAvailableFirstPolicy}.
 */
public final class MostAvailableFirstPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the worker with the most available space is chosen.
   */
  @Test
  public void getMostAvailableWorker() {
    List<BlockWorkerInfo> workerInfoList = new ArrayList<>();
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker1")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker2")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), 2 * (long) Constants.GB, 0));
    workerInfoList.add(new BlockWorkerInfo(new WorkerNetAddress().setHost("worker3")
        .setRpcPort(PORT).setDataPort(PORT).setWebPort(PORT), 3 * (long) Constants.GB, 0));
    MostAvailableFirstPolicy policy = new MostAvailableFirstPolicy(null);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workerInfoList).setBlockInfo(new BlockInfo().setLength(Constants.MB));
    Assert.assertEquals("worker3",
        policy.getWorker(options).getHost());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(MostAvailableFirstPolicy.class,
        new Class[]{AlluxioConfiguration.class},
        new Object[]{ConfigurationTestUtils.defaults()});
  }
}
