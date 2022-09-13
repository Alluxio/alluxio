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
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class CapacityBaseRandomPolicyTest {
  private final InstancedConfiguration mNoReplicaLimitConf = Configuration.copyGlobal();

  @Before
  public void before() {
    mNoReplicaLimitConf.set(PropertyKey.USER_FILE_REPLICATION_MAX, -1);
  }

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
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(7).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(9).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(10).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(70).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(109).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress4),
        buildPolicyWithTarget(110).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress4),
        buildPolicyWithTarget(700).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress4),
        buildPolicyWithTarget(1109).getWorker(getWorkerOptions));
    Optional<WorkerNetAddress> address = buildPolicyWithTarget(1109).getWorker(getWorkerOptions);
    Assert.assertTrue(address.isPresent());
    Assert.assertNotEquals(netAddress1, address.get());
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
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(7).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress1),
        buildPolicyWithTarget(99).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(100).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(156).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress2),
        buildPolicyWithTarget(199).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress3),
        buildPolicyWithTarget(200).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress3),
        buildPolicyWithTarget(211).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.of(netAddress3),
        buildPolicyWithTarget(299).getWorker(getWorkerOptions));
    Optional<WorkerNetAddress> address = buildPolicyWithTarget(299).getWorker(getWorkerOptions);
    Assert.assertTrue(address.isPresent());
    Assert.assertNotEquals(netAddress1, address.get());
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
    Assert.assertEquals(Optional.empty(), buildPolicyWithTarget(0).getWorker(getWorkerOptions));
    Assert.assertEquals(Optional.empty(), buildPolicyWithTarget(1009).getWorker(getWorkerOptions));
  }

  @Test
  public void getWorkerWithReplicaLimit() {
    InstancedConfiguration replicaLimitConf = Configuration.copyGlobal();
    replicaLimitConf.set(PropertyKey.USER_FILE_REPLICATION_MAX, 1);
    GetWorkerOptions getWorkerOptions = mockOptions();
    CapacityBaseRandomPolicy policy = new CapacityBaseRandomPolicy(replicaLimitConf);
    Set<WorkerNetAddress> addressSet = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      policy.getWorker(getWorkerOptions).ifPresent(addressSet::add);
    }
    Assert.assertEquals(1, addressSet.size());
  }

  @Test
  public void getWorkerWithoutReplicaLimit() {
    GetWorkerOptions getWorkerOptions = mockOptions();
    CapacityBaseRandomPolicy policy = new CapacityBaseRandomPolicy(mNoReplicaLimitConf);
    Set<WorkerNetAddress> addressSet = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      policy.getWorker(getWorkerOptions).ifPresent(addressSet::add);
    }
    Assert.assertTrue(addressSet.size() > 1);
  }

  private GetWorkerOptions mockOptions() {
    GetWorkerOptions getWorkerOptions = GetWorkerOptions.defaults();
    getWorkerOptions.setBlockWorkerInfos(mockWorkerList());
    getWorkerOptions.setBlockInfo(new BlockInfo().setBlockId(1L));
    return getWorkerOptions;
  }

  private ArrayList<BlockWorkerInfo> mockWorkerList() {
    ArrayList<BlockWorkerInfo> blockWorkerInfos = new ArrayList<>();
    WorkerNetAddress netAddress1 = new WorkerNetAddress().setHost("1");
    WorkerNetAddress netAddress2 = new WorkerNetAddress().setHost("2");
    WorkerNetAddress netAddress3 = new WorkerNetAddress().setHost("3");
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress1, 10, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress2, 100, 0));
    blockWorkerInfos.add(new BlockWorkerInfo(netAddress3, 1000, 0));
    return blockWorkerInfos;
  }

  /**
   * @param targetValue must be in [0,totalCapacity)
   */
  private CapacityBaseRandomPolicy buildPolicyWithTarget(final int targetValue) {
    return new CapacityBaseRandomPolicy(mNoReplicaLimitConf) {
      @Override
      protected long randomInCapacity(Long blockId, long totalCapacity) {
        return targetValue;
      }
    };
  }
}
