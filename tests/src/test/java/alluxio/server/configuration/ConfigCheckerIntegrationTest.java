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

package alluxio.server.configuration;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Scope;
import alluxio.client.MetaMasterClient;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.wire.MasterInfo;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test server-side configuration checker.
 */
public class ConfigCheckerIntegrationTest extends BaseIntegrationTest {
  private static final int TEST_NUM_MASTERS = 2;
  private static final int TEST_NUM_WORKERS = 2;

  @Test
  public void MultiMasters() throws Exception {
    Map<Integer, Map<PropertyKey, String>> masterProperties
        = generateProperties(TEST_NUM_MASTERS, Scope.MASTER);
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setClusterName("ConfigCheckerMultiMastersTest")
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(0)
        .setDeployMode(MultiProcessCluster.DeployMode.ZOOKEEPER_HA)
        .setMasterProperties(masterProperties)
        .build();
    try {
      cluster.start();
      MetaMasterClient client = cluster
          .waitForAllNodesRegistered(30 * Constants.SECOND_MS);
      Assert.assertEquals(TEST_NUM_MASTERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.CONF_MASTER_NUM)))
          .getConfMasterNum());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void MultiWorkers() throws Exception {
    Map<Integer, Map<PropertyKey, String>> workerProperties
        = generateProperties(TEST_NUM_WORKERS, PropertyKey.Scope.WORKER);
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setNumMasters(1)
        .setNumWorkers(TEST_NUM_WORKERS)
        .setWorkerProperties(workerProperties)
        .build();

    try {
      cluster.start();
      MetaMasterClient client = cluster
          .waitForAllNodesRegistered(30 * Constants.SECOND_MS);

      Assert.assertEquals(1, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.CONF_MASTER_NUM)))
          .getConfMasterNum());
      Assert.assertEquals(TEST_NUM_WORKERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.CONF_WORKER_NUM)))
          .getConfWorkerNum());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void MultiNodes() throws Exception {
    // Prepare properties
    Map<Integer, Map<PropertyKey, String>> properties = generateProperties(
        TEST_NUM_MASTERS + TEST_NUM_WORKERS, PropertyKey.Scope.SERVER);
    Map<Integer, Map<PropertyKey, String>> masterProperties = properties.entrySet().stream()
        .filter(entry -> (entry.getKey() < TEST_NUM_MASTERS))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<Integer, Map<PropertyKey, String>> workerProperties = properties.entrySet().stream()
        .filter(entry -> (entry.getKey() >= TEST_NUM_MASTERS))
        .collect(Collectors.toMap(entry -> entry.getKey() - TEST_NUM_MASTERS, Map.Entry::getValue));

    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setClusterName("ConfigCheckerMultiNodesTest")
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(TEST_NUM_WORKERS)
        .setDeployMode(MultiProcessCluster.DeployMode.ZOOKEEPER_HA)
        .setMasterProperties(masterProperties)
        .setWorkerProperties(workerProperties)
        .build();

    try {
      cluster.start();
      MetaMasterClient client = cluster.waitForAllNodesRegistered(
          30 * Constants.SECOND_MS);

      Assert.assertEquals(TEST_NUM_MASTERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.CONF_MASTER_NUM)))
          .getConfMasterNum());
      Assert.assertEquals(TEST_NUM_WORKERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.CONF_WORKER_NUM)))
          .getConfWorkerNum());
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  /**
   * Generates a map that different nodes contain different values for one property.
   *
   * @param nodeNum the number of nodes to test
   * @param scope the scope of the property key
   * @return generated properties
   */
  private Map<Integer, Map<PropertyKey, String>> generateProperties(int nodeNum,
      Scope scope) {
    Map<Integer, Map<PropertyKey, String>> properties = new HashMap<>();
    // Choose some example properties
    PropertyKey key = scope.equals(Scope.MASTER) ? PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS
        : scope.equals(Scope.WORKER) ? PropertyKey.WORKER_FREE_SPACE_TIMEOUT
        : PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS;
    for (int i = 0; i < nodeNum; i++) {
      Map<PropertyKey, String> prop = new HashMap<>();
      prop.put(key, ((Configuration.getMs(key) / Constants.SECOND_MS) + i) + "sec");
      properties.put(i, prop);
    }
    return properties;
  }
}
