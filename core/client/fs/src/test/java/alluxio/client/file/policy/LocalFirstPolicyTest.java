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
import static org.junit.Assert.assertNull;

import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.PropertyKey.Template;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.testing.EqualsTester;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link LocalFirstPolicy}.
 */
public final class LocalFirstPolicyTest {
  private static final int PORT = 1;

  /**
   * Tests that the local host is returned first.
   */
  @Test
  public void getLocalFirst() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.GB, localhostName, ""));
    assertEquals(localhostName, policy.getWorkerForNextBlock(workers, Constants.MB).getHost());
  }

  /**
   * Tests that another worker is picked in case the local host does not have enough space.
   */
  @Test
  public void getOthersWhenNotEnoughSpaceOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName();
    LocalFirstPolicy policy = new LocalFirstPolicy();
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.GB, "worker1", ""));
    workers.add(worker(Constants.MB, localhostName, ""));
    assertEquals("worker1", policy.getWorkerForNextBlock(workers, Constants.GB).getHost());
  }

  /**
   * Tests that non-local workers are randomly selected.
   */
  @Test
  public void getOthersRandomly() {
    LocalFirstPolicy policy = new LocalFirstPolicy();
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
  public void chooseClosestTier() {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "","node=node2,rack=rack3"));
    workers.add(worker(Constants.GB, "","node=node3,rack=rack2"));
    workers.add(worker(Constants.GB, "","node=node4,rack=rack3"));
    LocalFirstPolicy policy;
    WorkerNetAddress chosen;
    // local rack
    policy = new LocalFirstPolicy(TieredIdentity.fromString("node=node1,rack=rack2"));
    chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals("rack2", chosen.getTieredIdentity().getTiers().get(1).getValue());

    // local node
    policy =  new LocalFirstPolicy(TieredIdentity.fromString("node=node4,rack=rack3"));
    chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals("node4", chosen.getTieredIdentity().getTiers().get(0).getValue());
  }

  @Test
  public void respectStrictLocality() throws Exception {
    try (Closeable c = new ConfigurationRule(
        Template.LOCALITY_TIER_WAIT.format(Constants.LOCALITY_RACK), "-1").toResource()) {
      List<BlockWorkerInfo> workers = new ArrayList<>();
      workers.add(worker(Constants.GB, "", "node=node,rack=rack"));
      LocalFirstPolicy policy =
          new LocalFirstPolicy(TieredIdentity.fromString("node=other,rack=other"));
      WorkerNetAddress chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
      // Rack locality is set to strict, and no rack matches.
      assertNull(chosen);
    }
  }

  @Test
  public void tieredLocalityEnoughSpace() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    // Local node doesn't have enough space
    workers.add(worker(Constants.MB, "","node=node2,rack=rack3"));
    workers.add(worker(Constants.GB, "","node=node3,rack=rack2"));
    // Local rack has enough space
    workers.add(worker(Constants.GB, "","node=node4,rack=rack3"));
    LocalFirstPolicy policy = new LocalFirstPolicy(TieredIdentity.fromString("node=node2,rack=rack3"));
    WorkerNetAddress chosen = policy.getWorkerForNextBlock(workers, Constants.GB);
    assertEquals(workers.get(2).getNetAddress(), chosen);
  }

  @Test
  public void equalsTest() throws Exception {
    new EqualsTester()
        .addEqualityGroup(new LocalFirstPolicy(TieredIdentity.fromString("node=x,rack=y")))
        .addEqualityGroup(new LocalFirstPolicy(TieredIdentity.fromString("node=x,rack=z")))
        .testEquals();
  }

  private BlockWorkerInfo worker(long capacity, String hostname, String tieredIdentity) {
    WorkerNetAddress address = new WorkerNetAddress();
    if (tieredIdentity != null && !tieredIdentity.isEmpty()) {
      address.setTieredIdentity(TieredIdentity.fromString(tieredIdentity));
    }
    if (hostname != null && !hostname.isEmpty()) {
      address.setHost(hostname);
    }
    return new BlockWorkerInfo(address, capacity, 0);
  }
}
