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

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class CapacityBaseRandomPolicyTest {

  @Test
  public void getWorkerDifferentCapacity() {
    GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults();
    ArrayList<BlockWorkerInfo> blockWorkerInfos = new ArrayList<>();
    WorkerNetAddress netAddress1 = new WorkerNetAddress().setHost("1");
    WorkerNetAddress netAddress2 = new WorkerNetAddress().setHost("2");
    WorkerNetAddress netAddress3 = new WorkerNetAddress().setHost("3");
    WorkerNetAddress netAddress4 = new WorkerNetAddress().setHost("4");
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress1, 10, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress2, 100, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress3, 0, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress4, 1000, 0));
    getWorkerOptions.setBlockWorkerInfos(blockWorkerInfos);
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(7).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(9).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(10).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(70).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(109).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress4, buildPolicyWithTarget(110).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress4, buildPolicyWithTarget(700).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress4, buildPolicyWithTarget(1109).getWorker(getWorkerOptions));
    Assert.assertNotEquals(netAddress1, buildPolicyWithTarget(1109).getWorker(getWorkerOptions));
  }

  @Test
  public void getWorkerSameCapacity() {
    GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults();
    ArrayList<BlockWorkerInfo> blockWorkerInfos = new ArrayList<>();
    WorkerNetAddress netAddress1 = new WorkerNetAddress().setHost("1");
    WorkerNetAddress netAddress2 = new WorkerNetAddress().setHost("2");
    WorkerNetAddress netAddress3 = new WorkerNetAddress().setHost("3");
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress1, 100, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress2, 100, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress3, 100, 0));
    getWorkerOptions.setBlockWorkerInfos(blockWorkerInfos);
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(7).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress1, buildPolicyWithTarget(99).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(100).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(156).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress2, buildPolicyWithTarget(199).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress3, buildPolicyWithTarget(200).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress3, buildPolicyWithTarget(211).getWorker(getWorkerOptions));
    Assert.assertEquals(netAddress3, buildPolicyWithTarget(299).getWorker(getWorkerOptions));
    Assert.assertNotEquals(netAddress1, buildPolicyWithTarget(299).getWorker(getWorkerOptions));
  }

  @Test
  public void testNoMatchWorker() {
    GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults();
    ArrayList<BlockWorkerInfo> blockWorkerInfos = new ArrayList<>();
    WorkerNetAddress netAddress1 = new WorkerNetAddress();
    WorkerNetAddress netAddress2 = new WorkerNetAddress();
    WorkerNetAddress netAddress3 = new WorkerNetAddress();
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress1, 0, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress2, 0, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress3, 0, 0));
    getWorkerOptions.setBlockWorkerInfos(blockWorkerInfos);
    Assert.assertNull(buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertNull(buildPolicyWithTarget(1009).getWorker(getWorkerOptions));
  }

  /**
   * @param targetValue must be in [0,totalCapacity)
   */
  private CapacityBaseRandomPolicy buildPolicyWithTarget(final int targetValue) {
    return new CapacityBaseRandomPolicy(null) {
      @Override
      protected long randomInCapacity(long totalCapacity) {
        return targetValue;
      }
    };
  }
}
