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

import static alluxio.client.util.ClientTestUtils.worker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.testing.EqualsTester;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link LocalFirstPolicy}.
 */
public final class LocalFirstPolicyTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();
  private static int sResolutionTimeout =
      (int) sConf.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS);

  /**
   * Tests that the local host is returned first.
   */
  @Test
  public void getLocalFirst() {
    String localhostName =
        NetworkAddressUtils.getLocalHostName(sResolutionTimeout);
    LocalFirstPolicy policy = new LocalFirstPolicy(sConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.GB, localhostName, ""));
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.MB));
    assertEquals(localhostName, policy.getWorker(options).getHost());
  }

  /**
   * Tests that another worker is picked in case the local host does not have enough capacity.
   */
  @Test
  public void getOthersWhenNotEnoughCapacityOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName(sResolutionTimeout);
    LocalFirstPolicy policy = new LocalFirstPolicy(sConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.MB, localhostName, ""));
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.GB));
    assertEquals("worker1", policy.getWorker(options).getHost());
  }

  /**
   * Tests that non-local workers are randomly selected.
   */
  @Test
  public void getOthersRandomly() {
    LocalFirstPolicy policy = new LocalFirstPolicy(sConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.GB, "worker2", ""));

    boolean success = false;
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.MB));
    for (int i = 0; i < 100; i++) {
      String host = policy.getWorker(options).getHost();
      if (!host.equals(policy.getWorker(options).getHost())) {
        success = true;
        break;
      }
    }
    assertTrue(success);
  }

  @Test
  public void chooseClosestTier() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "node2", "rack3"));
    workers.add(worker(Constants.GB, "node3", "rack2"));
    workers.add(worker(Constants.GB, "node4", "rack3"));
    LocalFirstPolicy policy;
    WorkerNetAddress chosen;
    // local rack
    policy = new LocalFirstPolicy(TieredIdentityFactory.fromString("node=node1,rack=rack2",
        sConf), sConf);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.GB));
    chosen = policy.getWorker(options);
    assertEquals("rack2", chosen.getTieredIdentity().getTier(1).getValue());

    // local node
    policy = new LocalFirstPolicy(TieredIdentityFactory.fromString("node=node4,rack=rack3",
        sConf),
        sConf);
    chosen = policy.getWorker(options);
    assertEquals("node4", chosen.getTieredIdentity().getTier(0).getValue());
  }

  @Test
  public void tieredLocalityEnoughSpace() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    // Local node doesn't have enough space
    workers.add(worker(Constants.MB, "node2", "rack3"));
    workers.add(worker(Constants.GB, "node3", "rack2"));
    // Local rack has enough space
    workers.add(worker(Constants.GB, "node4", "rack3"));
    LocalFirstPolicy policy = new LocalFirstPolicy(TieredIdentityFactory
        .fromString("node=node2,rack=rack3", sConf), sConf);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.GB));
    WorkerNetAddress chosen = policy.getWorker(options);
    assertEquals(workers.get(2).getNetAddress(), chosen);
  }

  @Test
  public void equalsTest() throws Exception {
    new EqualsTester()
        .addEqualityGroup(new LocalFirstPolicy(TieredIdentityFactory.fromString("node=x,rack=y",
            sConf), sConf))
        .addEqualityGroup(new LocalFirstPolicy(TieredIdentityFactory.fromString("node=x,rack=z",
            sConf), sConf))
        .testEquals();
  }
}
