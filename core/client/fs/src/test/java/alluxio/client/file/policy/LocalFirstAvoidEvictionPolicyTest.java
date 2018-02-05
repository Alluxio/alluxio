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

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.network.TieredIdentityFactory;
import alluxio.test.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.TieredIdentity.LocalityTier;
import alluxio.wire.WorkerNetAddress;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link LocalFirstAvoidEvictionPolicy}. The class delegates to {@link LocalFirstPolicy}, so
 * most of its functionality is tested in {@link LocalFirstPolicyTest}.
 */
public class LocalFirstAvoidEvictionPolicyTest {

  @Test
  public void chooseClosestTierAvoidEviction() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, Constants.MB, "node2", "rack3"));
    workers.add(worker(Constants.GB, 0, "node3", "rack2"));
    workers.add(worker(Constants.GB, 0, "node4", "rack3"));
    FileWriteLocationPolicy policy;
    WorkerNetAddress chosen;
    // local rack with enough availability
    policy = new LocalFirstAvoidEvictionPolicy(
        TieredIdentityFactory.fromString("node=node2,rack=rack3"));
    chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals("node4", chosen.getTieredIdentity().getTier(0).getValue());
  }

  /**
   * Tests that another worker is picked in case the local host does not have enough availability.
   */
  @Test
  public void getOthersWhenNotEnoughAvailabilityOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    FileWriteLocationPolicy policy = new LocalFirstAvoidEvictionPolicy();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, 0, "worker1", ""));
    workers.add(worker(Constants.MB, Constants.MB, localhostName, ""));
    assertEquals("worker1", policy.getWorkerForNextBlock(workers, Constants.MB).getHost());
  }

  /**
   * Tests that local host is picked if none of the workers has enough availability.
   */
  @Test
  public void getLocalWhenNoneHasAvailability() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    FileWriteLocationPolicy policy = new LocalFirstAvoidEvictionPolicy();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, Constants.MB, "worker1", ""));
    workers.add(worker(Constants.GB, Constants.MB, localhostName, ""));
    assertEquals(localhostName,
        policy.getWorkerForNextBlock(workers, Constants.GB).getHost());
  }

  @Test
  public void equalsTest() throws Exception {
    CommonUtils.testEquals(LocalFirstAvoidEvictionPolicy.class);
  }

  private BlockWorkerInfo worker(long capacity, long used, String node, String rack) {
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
    return new BlockWorkerInfo(address, capacity, used);
  }
}
