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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.membership.WorkerClusterView;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class LocalWorkerPolicyTest {
  private static final String LOCAL_HOSTNAME = "localhost";
  private static final WorkerIdentity LOCAL_WORKER_ID =
      WorkerIdentityTestUtils.randomLegacyId();

  InstancedConfiguration mConf;

  @Before
  public void setup() {
    mConf = new InstancedConfiguration(Configuration.copyProperties());
    mConf.set(PropertyKey.USER_WORKER_SELECTION_POLICY,
        "LOCAL");
    mConf.set(PropertyKey.USER_HOSTNAME, LOCAL_HOSTNAME);
  }

  @Test
  public void getOneWorker() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof LocalWorkerPolicy);
    // Prepare a worker list
    WorkerClusterView workers = new WorkerClusterView(Arrays.asList(
        new WorkerInfo()
            .setIdentity(LOCAL_WORKER_ID)
            .setAddress(new WorkerNetAddress()
                .setHost(LOCAL_HOSTNAME).setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0),
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
            .setAddress(new WorkerNetAddress()
                .setHost("remotehost").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0)));

    List<BlockWorkerInfo> assignedWorkers = policy.getPreferredWorkers(workers, "hdfs://a/b/c", 1);
    assertEquals(1, assignedWorkers.size());
    assertTrue(contains(workers, assignedWorkers.get(0)));
    assertEquals(LOCAL_WORKER_ID, assignedWorkers.get(0).getIdentity());
    assertEquals(LOCAL_HOSTNAME, assignedWorkers.get(0).getNetAddress().getHost());

    assertThrows(ResourceExhaustedException.class, () -> {
      // Getting 1 out of no workers will result in an error
      policy.getPreferredWorkers(new WorkerClusterView(ImmutableList.of()), "hdfs://a/b/c", 1);
    });
  }

  @Test
  public void getMultipleWorkers() throws Exception {
    WorkerLocationPolicy policy = WorkerLocationPolicy.Factory.create(mConf);
    assertTrue(policy instanceof LocalWorkerPolicy);
    // Prepare a worker list

    WorkerClusterView workers = new WorkerClusterView(Arrays.asList(
        new WorkerInfo()
            .setIdentity(LOCAL_WORKER_ID)
            .setAddress(new WorkerNetAddress()
                .setHost(LOCAL_HOSTNAME).setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0),
        new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
            .setAddress(new WorkerNetAddress()
                .setHost("master2").setRpcPort(29998).setDataPort(29999).setWebPort(30000))
            .setCapacityBytes(1024)
            .setUsedBytes(0)));

    assertThrows(ResourceExhaustedException.class, () -> {
      // There is only one local worker and getting 2 will get an error
      policy.getPreferredWorkers(workers, "hdfs://a/b/c", 2);
    });

    WorkerClusterView threeWorkerCluster = new WorkerClusterView(Iterables.concat(workers,
        ImmutableList.of(new WorkerInfo()
            .setIdentity(WorkerIdentityTestUtils.randomLegacyId())
            .setAddress(new WorkerNetAddress()
                .setHost(LOCAL_HOSTNAME).setRpcPort(30001).setDataPort(30002).setWebPort(30003))
            .setCapacityBytes(1024)
            .setUsedBytes(0))));

    List<BlockWorkerInfo> localWorkers =
        policy.getPreferredWorkers(threeWorkerCluster, "hdfs://a/b/c", 2);
    assertEquals(2, localWorkers.size());
    assertNotEquals(localWorkers.get(0).getNetAddress(), localWorkers.get(1).getNetAddress());
    assertNotEquals(localWorkers.get(0).getIdentity(), localWorkers.get(1).getIdentity());
    assertEquals(LOCAL_HOSTNAME, localWorkers.get(0).getNetAddress().getHost());
    assertEquals(LOCAL_HOSTNAME, localWorkers.get(1).getNetAddress().getHost());
  }

  private boolean contains(WorkerClusterView workers, BlockWorkerInfo targetWorker) {
    // BlockWorkerInfo's equality is delegated to the WorkerNetAddress
    return workers.stream().anyMatch(w ->
        w.getAddress().equals(targetWorker.getNetAddress()));
  }
}
