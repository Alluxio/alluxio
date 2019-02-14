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

package alluxio.client.file.policy;

import static org.junit.Assert.assertEquals;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;
import alluxio.wire.WorkerNetAddress;

import com.google.common.testing.EqualsTester;
import org.junit.Assert;
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
    assertEquals(localhostName, policy.getWorkerForNextBlock(workers, Constants.MB).getHost());
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
    assertEquals("worker1", policy.getWorkerForNextBlock(workers, Constants.GB).getHost());
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
    for (int i = 0; i < 100; i++) {
      String host = policy.getWorkerForNextBlock(workers, Constants.GB).getHost();
      if (!host.equals(policy.getWorkerForNextBlock(workers, Constants.GB).getHost())) {
        success = true;
        break;
      }
    }
    Assert.assertTrue(success);
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
    policy = LocalFirstPolicy.create(TieredIdentityFactory.fromString("node=node1,rack=rack2",
        sConf), sConf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP));
    chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals("rack2", chosen.getTieredIdentity().getTier(1).getValue());

    // local node
    policy = LocalFirstPolicy.create(TieredIdentityFactory.fromString("node=node4,rack=rack3",
        sConf),
        sConf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP));
    chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
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
    LocalFirstPolicy policy =
        LocalFirstPolicy.create(TieredIdentityFactory.fromString("node=node2,rack=rack3", sConf),
            sConf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP));
    WorkerNetAddress chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals(workers.get(2).getNetAddress(), chosen);
  }

  @Test
  public void equalsTest() throws Exception {
    new EqualsTester()
        .addEqualityGroup(
            LocalFirstPolicy.create(TieredIdentityFactory.fromString("node=x,rack=y", sConf),
                sConf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP)))
        .addEqualityGroup(
            LocalFirstPolicy.create(TieredIdentityFactory.fromString("node=x,rack=z", sConf),
                sConf.getBoolean(PropertyKey.LOCALITY_COMPARE_NODE_IP)))
        .testEquals();
  }

  private BlockWorkerInfo worker(long capacity, String node, String rack) {
    WorkerNetAddress address = new WorkerNetAddress();
    List<LocalityTier> tiers = new ArrayList<>();
    if (node != null && !node.isEmpty()) {
      address.setHost(node);
      tiers.add(new LocalityTier(Constants.LOCALITY_NODE, node));
    }
    if (rack != null && !rack.isEmpty()) {
      tiers.add(new LocalityTier(Constants.LOCALITY_RACK, rack));
    }
    address.setTieredIdentity(new TieredIdentity(tiers));
    return new BlockWorkerInfo(address, capacity, 0);
  }
}
