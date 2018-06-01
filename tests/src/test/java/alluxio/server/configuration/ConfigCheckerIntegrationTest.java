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
import alluxio.client.MetaMasterClient;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigCheckReport.ConfigStatus;
import alluxio.wire.MasterInfo;

import org.hamcrest.CoreMatchers;
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
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
  private static final int TEST_NUM_MASTERS = 2;
  private static final int TEST_NUM_WORKERS = 2;

  @Test
  public void MultiMasters() throws Exception {
    PropertyKey key = PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS;
    Map<Integer, Map<PropertyKey, String>> masterProperties
        = generatePropertyWithDifferentValues(TEST_NUM_MASTERS, key);
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
          .waitForAllNodesRegistered(WAIT_TIMEOUT_MS);
      Assert.assertEquals(TEST_NUM_MASTERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.MASTER_ADDRESSES)))
          .getMasterAddresses().size());

      ConfigCheckReport report = client.getConfigReport();
      Assert.assertEquals(ConfigStatus.WARN, report.getConfigStatus());
      Assert.assertThat(report.getConfigWarns().toString(),
          CoreMatchers.containsString(key.getName()));
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void MultiWorkers() throws Exception {
    PropertyKey key = PropertyKey.WORKER_FREE_SPACE_TIMEOUT;
    Map<Integer, Map<PropertyKey, String>> workerProperties
        = generatePropertyWithDifferentValues(TEST_NUM_WORKERS, key);
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setNumMasters(1)
        .setNumWorkers(TEST_NUM_WORKERS)
        .setWorkerProperties(workerProperties)
        .build();

    try {
      cluster.start();
      MetaMasterClient client = cluster
          .waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

      Assert.assertEquals(1, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.MASTER_ADDRESSES)))
          .getMasterAddresses().size());
      Assert.assertEquals(TEST_NUM_WORKERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.WORKER_ADDRESSES)))
          .getWorkerAddresses().size());

      ConfigCheckReport report = client.getConfigReport();
      Assert.assertEquals(ConfigStatus.WARN, report.getConfigStatus());
      Assert.assertThat(report.getConfigWarns().toString(),
          CoreMatchers.containsString(key.getName()));
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  @Test
  public void MultiNodes() throws Exception {
    PropertyKey key = PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS;
    // Prepare properties
    Map<Integer, Map<PropertyKey, String>> properties = generatePropertyWithDifferentValues(
        TEST_NUM_MASTERS + TEST_NUM_WORKERS, key);
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
      MetaMasterClient client = cluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);

      Assert.assertEquals(TEST_NUM_MASTERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.MASTER_ADDRESSES)))
          .getMasterAddresses().size());
      Assert.assertEquals(TEST_NUM_WORKERS, client.getMasterInfo(
          new HashSet<>(Arrays.asList(MasterInfo.MasterInfoField.WORKER_ADDRESSES)))
          .getWorkerAddresses().size());

      ConfigCheckReport report = client.getConfigReport();
      Assert.assertEquals(ConfigStatus.FAILED, report.getConfigStatus());
      Assert.assertThat(report.getConfigErrors().toString(),
          CoreMatchers.containsString(key.getName()));
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  /**
   * Generates a map that different nodes contain different values for one property.
   *
   * @param nodeNum the number of nodes to test
   * @param key the time-related property key to generate values
   * @return generated properties
   */
  private Map<Integer, Map<PropertyKey, String>> generatePropertyWithDifferentValues(
      int nodeNum, PropertyKey key) {
    Map<Integer, Map<PropertyKey, String>> properties = new HashMap<>();
    for (int i = 0; i < nodeNum; i++) {
      Map<PropertyKey, String> prop = new HashMap<>();
      prop.put(key, ((Configuration.getMs(key) / Constants.SECOND_MS) + i) + "sec");
      properties.put(i, prop);
    }
    return properties;
  }
}
