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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConsistentHashPolicyTest {
  private final List<BlockWorkerInfo> mWorkerInfos = new ArrayList<>();
  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  @Before
  public void before() {
    mWorkerInfos.clear();
    for (int i = 0; i < 10; i++) {
      mWorkerInfos.add(new BlockWorkerInfo(
          new WorkerNetAddress().setHost("worker" + i).setRpcPort(8080).setDataPort(8080)
              .setWebPort(8080), Constants.GB, 0));
    }
    sConf = ConfigurationTestUtils.defaults();
  }

  @Test
  public void getSingleWorkerConsistently() {
    ConsistentHashPolicy policy = (ConsistentHashPolicy) BlockLocationPolicy.Factory.create(
        ConsistentHashPolicy.class, sConf);
    String host = policy.getWorker(GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
        .setBlockInfo(new BlockInfo().setBlockId(1))).getHost();
    for (int i = 0; i < 10; i++) {
      // For the same block, always return the same worker.
      Collections.shuffle(mWorkerInfos);
      assertEquals(host, policy.getWorker(
              GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
                  .setBlockInfo(new BlockInfo().setBlockId(1)))
          .getHost());
    }
  }

  @Test
  public void getMultiplePreferredWorkerConsistently() {
    ConsistentHashPolicy policy = (ConsistentHashPolicy) BlockLocationPolicy.Factory.create(
        ConsistentHashPolicy.class, sConf);
    Set<BlockWorkerInfo> preferredWorkers =
        policy.getWorkers(GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
            .setBlockInfo(new BlockInfo().setBlockId(1)), 3);
    assertEquals(3, preferredWorkers.size());
    for (int i = 0; i < 10; i++) {
      // For the same block, always return the same worker.
      Collections.shuffle(mWorkerInfos);
      assertEquals(preferredWorkers,
          policy.getWorkers(GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
              .setBlockInfo(new BlockInfo().setBlockId(1)), 3));
    }
  }

  @Test
  public void distributionTest() {
    ConsistentHashPolicy policy = (ConsistentHashPolicy) BlockLocationPolicy.Factory.create(
        ConsistentHashPolicy.class, sConf);
    Map<WorkerNetAddress, Integer> result = new HashMap<>();
    for (int i = 0; i < 1_000_000; i++) {
      WorkerNetAddress workerAddr = policy.getWorker(
          GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
              .setBlockInfo(new BlockInfo().setBlockId(i)));
      result.put(workerAddr, result.getOrDefault(workerAddr, 0) + 1);
    }
    assertTrue(result.values().stream().allMatch(count -> count >= 80000 && count <= 120000));
  }

  @Test
  public void insufficientWorkerTest() {
    ConsistentHashPolicy policy = (ConsistentHashPolicy) BlockLocationPolicy.Factory.create(
        ConsistentHashPolicy.class, sConf);
    //Request 3 times as many workers as available workers
    Set<BlockWorkerInfo> preferredWorkers =
        policy.getWorkers(GetWorkerOptions.defaults().setBlockWorkerInfos(mWorkerInfos)
            .setBlockInfo(new BlockInfo().setBlockId(1)), mWorkerInfos.size() * 3);
    assertEquals(mWorkerInfos.size(), preferredWorkers.size());
    //Request workers when there is no available workers
    preferredWorkers =
        policy.getWorkers(
            GetWorkerOptions.defaults().setBlockWorkerInfos(Collections.EMPTY_LIST)
                .setBlockInfo(new BlockInfo().setBlockId(1)), 3);
    assertEquals(0, preferredWorkers.size());
    assertNull(
        policy.getWorker(GetWorkerOptions.defaults().setBlockWorkerInfos(Collections.EMPTY_LIST)
            .setBlockInfo(new BlockInfo().setBlockId(1))));
  }
}
