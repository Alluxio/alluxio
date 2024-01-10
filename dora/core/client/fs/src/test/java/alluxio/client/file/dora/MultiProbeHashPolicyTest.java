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

package alluxio.client.file.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.membership.WorkerClusterView;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.WorkerState;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MultiProbeHashPolicyTest {
  InstancedConfiguration mConf;

  @Before
  public void setup() {
    mConf = new InstancedConfiguration(Configuration.copyProperties());
    mConf.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "MULTI_PROBE");
  }

  @Test
  public void getOneWorker() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof MultiProbeHashPolicy);
    // Prepare a worker list
    WorkerClusterView workers = new WorkerClusterView(Arrays.asList(
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.ofLegacyId(1))
            .setAddress(new WorkerNetAddress()
                .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0),
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.ofLegacyId(2))
            .setAddress(new WorkerNetAddress()
                .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0)));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 1);
    assertEquals(1, assignedWorkers.size());
    assertTrue(contains(workers, assignedWorkers.get(0)));

    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 1 out of no workers will result in an error
      policy.getPreferredWorkers(new WorkerClusterView(ImmutableList.of()), "hdfs://a/b/c", 1);
    });
  }

  @Test
  public void getMultipleWorkers() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof MultiProbeHashPolicy);
    // Prepare a worker list
    WorkerClusterView workers = new WorkerClusterView(Arrays.asList(
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.ofLegacyId(1))
            .setAddress(new WorkerNetAddress()
                .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0),
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.ofLegacyId(2))
            .setAddress(new WorkerNetAddress()
                .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0)));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 2);
    assertEquals(2, assignedWorkers.size());
    assertTrue(assignedWorkers.stream().allMatch(w -> contains(workers, w)));
    // The order of the workers should be consistent
    assertEquals(assignedWorkers.get(0).getNetAddress().getHost(), "master1");
    assertEquals(assignedWorkers.get(1).getNetAddress().getHost(), "master2");
    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 2 out of 1 worker will result in an error
      policy.getPreferredWorkers(
          new WorkerClusterView(Arrays.asList(
              new WorkerInfo()
                  .setIdentity(WorkerIdentityTestUtils.ofLegacyId(1))
                  .setAddress(new WorkerNetAddress()
                      .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
                  .setCapacityBytes(1024)
                  .setUsedBytes(0))),
          "hdfs://a/b/c", 2);
    });
  }

  /**
   * Tests that the policy returns latest worker address even though the worker's ID
   * has stayed the same during the refresh interval.
   */
  @Test
  public void workerAddrUpdateWithIdUnchanged() throws Exception {
    MultiProbeHashPolicy policy = new MultiProbeHashPolicy(mConf);
    List<WorkerInfo> workers = new ArrayList<>();
    workers.add(new WorkerInfo().setIdentity(WorkerIdentityTestUtils.ofLegacyId(1L))
        .setAddress(new WorkerNetAddress().setHost("host1"))
        .setCapacityBytes(0)
        .setUsedBytes(0)
        .setState(WorkerState.LIVE));
    workers.add(new WorkerInfo().setIdentity(WorkerIdentityTestUtils.ofLegacyId(2L))
        .setAddress(new WorkerNetAddress().setHost("host2"))
        .setCapacityBytes(0)
        .setUsedBytes(0)
        .setState(WorkerState.LIVE));
    List<BlockWorkerInfo> selectedWorkers =
        policy.getPreferredWorkers(new WorkerClusterView(workers), "fileId", 2);
    assertEquals("host1",
        selectedWorkers.stream()
            .filter(w -> w.getIdentity().equals(WorkerIdentityTestUtils.ofLegacyId(1L)))
            .findFirst()
            .get()
            .getNetAddress()
            .getHost());

    // now the worker 1 has migrated to host 3
    workers.set(0, new WorkerInfo().setIdentity(WorkerIdentityTestUtils.ofLegacyId(1L))
        .setAddress(new WorkerNetAddress().setHost("host3"))
        .setCapacityBytes(0)
        .setUsedBytes(0)
        .setState(WorkerState.LIVE));
    List<BlockWorkerInfo> updatedWorkers =
        policy.getPreferredWorkers(new WorkerClusterView(workers), "fileId", 2);
    assertEquals(
        selectedWorkers.stream().map(BlockWorkerInfo::getIdentity).collect(Collectors.toList()),
        updatedWorkers.stream().map(BlockWorkerInfo::getIdentity).collect(Collectors.toList()));
    assertEquals("host3",
        updatedWorkers.stream()
            .filter(w -> w.getIdentity().equals(WorkerIdentityTestUtils.ofLegacyId(1L)))
            .findFirst()
            .get()
            .getNetAddress()
            .getHost());
  }

  private boolean contains(WorkerClusterView workers, BlockWorkerInfo targetWorker) {
    // BlockWorkerInfo's equality is delegated to the WorkerNetAddress
    return workers.stream().anyMatch(w ->
        w.getAddress().equals(targetWorker.getNetAddress()));
  }
}
