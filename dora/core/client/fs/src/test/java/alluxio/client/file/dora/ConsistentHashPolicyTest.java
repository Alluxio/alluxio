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
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConsistentHashPolicyTest {
  InstancedConfiguration mConf;

  @Before
  public void setup() {
    mConf = new InstancedConfiguration(Configuration.copyProperties());
    mConf.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "alluxio.client.file.dora.ConsistentHashPolicy");
  }

  @Test
  public void getOneWorker() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof ConsistentHashPolicy);
    // Prepare a worker list
    List<BlockWorkerInfo> workers = new ArrayList<>();
    WorkerNetAddress workerAddr1 = new WorkerNetAddress()
        .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(
        WorkerIdentityTestUtils.ofLegacyId(1), workerAddr1, 1024, 0));
    WorkerNetAddress workerAddr2 = new WorkerNetAddress()
        .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(
        WorkerIdentityTestUtils.ofLegacyId(2), workerAddr2, 1024, 0));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 1);
    assertEquals(1, assignedWorkers.size());
    assertTrue(contains(workers, assignedWorkers.get(0)));

    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 1 out of no workers will result in an error
      policy.getPreferredWorkers(ImmutableList.of(), "hdfs://a/b/c", 1);
    });
  }

  @Test
  public void getMultipleWorkers() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof ConsistentHashPolicy);
    // Prepare a worker list
    List<BlockWorkerInfo> workers = new ArrayList<>();
    WorkerNetAddress workerAddr1 = new WorkerNetAddress()
        .setHost("master1").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    final WorkerIdentity identity = WorkerIdentityTestUtils.ofLegacyId(1);
    workers.add(new BlockWorkerInfo(
        identity, workerAddr1, 1024, 0));
    WorkerNetAddress workerAddr2 = new WorkerNetAddress()
        .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000);
    workers.add(new BlockWorkerInfo(
        WorkerIdentityTestUtils.ofLegacyId(2), workerAddr2, 1024, 0));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 2);
    assertEquals(2, assignedWorkers.size());
    assertTrue(assignedWorkers.stream().allMatch(w -> contains(workers, w)));
    // The order of the workers should be consistent
    assertEquals(assignedWorkers.get(0).getNetAddress().getHost(), workerAddr1.getHost());
    assertEquals(assignedWorkers.get(1).getNetAddress().getHost(), workerAddr2.getHost());
    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 2 out of 1 worker will result in an error
      policy.getPreferredWorkers(
          ImmutableList.of(new BlockWorkerInfo(identity, workerAddr1, 1024, 0)),
          "hdfs://a/b/c", 2);
    });
  }

  /**
   * Tests that the policy returns latest worker address even though the worker's ID
   * has stayed the same during the refresh interval.
   */
  @Test
  public void workerAddrUpdateWithIdUnchanged() throws Exception {
    ConsistentHashPolicy policy = new ConsistentHashPolicy(mConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(new BlockWorkerInfo(WorkerIdentityTestUtils.ofLegacyId(1L),
        new WorkerNetAddress().setHost("host1"), 0, 0));
    workers.add(new BlockWorkerInfo(WorkerIdentityTestUtils.ofLegacyId(2L),
        new WorkerNetAddress().setHost("host2"), 0, 0));
    List<BlockWorkerInfo> selectedWorkers =
        policy.getPreferredWorkers(ImmutableList.copyOf(workers), "fileId", 2);
    assertEquals("host1",
        selectedWorkers.stream()
            .filter(w -> w.getIdentity().equals(WorkerIdentityTestUtils.ofLegacyId(1L)))
            .findFirst()
            .get()
            .getNetAddress()
            .getHost());

    // now the worker 1 has migrated to host 3
    workers.set(0, new BlockWorkerInfo(WorkerIdentityTestUtils.ofLegacyId(1L),
        new WorkerNetAddress().setHost("host3"), 0, 0));
    List<BlockWorkerInfo> updatedWorkers =
        policy.getPreferredWorkers(ImmutableList.copyOf(workers), "fileId", 2);
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

  private boolean contains(List<BlockWorkerInfo> workers, BlockWorkerInfo targetWorker) {
    // BlockWorkerInfo's equality is delegated to the WorkerNetAddress
    return workers.stream().anyMatch(w ->
        w.getNetAddress().equals(targetWorker.getNetAddress()));
  }
}
