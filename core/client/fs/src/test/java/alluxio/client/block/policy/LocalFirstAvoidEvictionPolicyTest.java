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
 * Tests {@link LocalFirstAvoidEvictionPolicy}. The class delegates to {@link LocalFirstPolicy}, so
 * most of its functionality is tested in {@link LocalFirstPolicyTest}.
 */
public class LocalFirstAvoidEvictionPolicyTest {

  private InstancedConfiguration mConf = ConfigurationTestUtils.defaults();

  @Test
  public void chooseClosestTierAvoidEviction() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, Constants.MB, "node2", "rack3"));
    workers.add(worker(Constants.GB, 0, "node3", "rack2"));
    workers.add(worker(Constants.GB, 0, "node4", "rack3"));
    BlockLocationPolicy policy;
    WorkerNetAddress chosen;
    // local rack with enough availability
    policy = new LocalFirstAvoidEvictionPolicy(
        mConf.getBytes(PropertyKey.USER_BLOCK_AVOID_EVICTION_POLICY_RESERVED_BYTES),
        TieredIdentityFactory.fromString("node=node2,rack=rack3", mConf), mConf);
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.GB));
    chosen = policy.getWorker(options);
    assertEquals("node4", chosen.getTieredIdentity().getTier(0).getValue());
  }

  /**
   * Tests that another worker is picked in case the local host does not have enough availability.
   */
  @Test
  public void getOthersWhenNotEnoughAvailabilityOnLocal() {
    String localhostName = NetworkAddressUtils.getLocalHostName(1000);
    BlockLocationPolicy policy = new LocalFirstAvoidEvictionPolicy(mConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, 0, "worker1", ""));
    workers.add(worker(Constants.MB, Constants.MB, localhostName, ""));
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.MB));
    assertEquals("worker1", policy.getWorker(options).getHost());
  }

  /**
   * Tests that local host is picked if none of the workers has enough availability.
   */
  @Test
  public void getLocalWhenNoneHasAvailability() {
    String localhostName = NetworkAddressUtils.getLocalHostName(1000);
    BlockLocationPolicy policy = new LocalFirstAvoidEvictionPolicy(mConf);
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, Constants.MB, "worker1", ""));
    workers.add(worker(Constants.GB, Constants.MB, localhostName, ""));
    GetWorkerOptions options = GetWorkerOptions.defaults()
        .setBlockWorkerInfos(workers).setBlockInfo(new BlockInfo().setLength(Constants.GB));
    assertEquals(localhostName,
        policy.getWorker(options).getHost());
  }

  @Test
  public void equalsTest() throws Exception {
    new EqualsTester()
        .addEqualityGroup(
            new LocalFirstAvoidEvictionPolicy(mConf),
            new LocalFirstAvoidEvictionPolicy(mConf))
        .testEquals();
  }
}
