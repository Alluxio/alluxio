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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.ConfigStatus;
import alluxio.grpc.Scope;
import alluxio.master.journal.JournalType;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.multi.process.MultiProcessCluster.DeployMode;
import alluxio.multi.process.PortCoordination;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.InconsistentProperty;

import com.google.common.collect.ImmutableMap;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Test server-side configuration checker.
 */
public class ConfigCheckerIntegrationTest extends BaseIntegrationTest {
  private static final int WAIT_TIMEOUT_MS = 60 * Constants.SECOND_MS;
  private static final int TEST_NUM_MASTERS = 2;
  private static final int TEST_NUM_WORKERS = 2;

  public MultiProcessCluster mCluster;

  @After
  public void after() throws Exception {
    mCluster.destroy();
  }

  @Test
  public void multiMasters() throws Exception {
    PropertyKey key = PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS;
    Map<Integer, Map<PropertyKey, String>> masterProperties
        = generatePropertyWithDifferentValues(TEST_NUM_MASTERS, key);
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_MASTERS)
        .setClusterName("ConfigCheckerMultiMastersTest")
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(0)
        .setMasterProperties(masterProperties)
        .build();
    mCluster.start();
    ConfigCheckReport report = getReport();
    // When using embedded journal, the journal paths are different
    assertEquals(mCluster.getDeployMode().equals(DeployMode.ZOOKEEPER_HA)
        ? ConfigStatus.WARN : ConfigStatus.FAILED, report.getConfigStatus());
    assertThat(report.getConfigWarns().toString(),
        CoreMatchers.containsString(key.getName()));
    mCluster.notifySuccess();
  }

  @Test
  public void multiMastersEmbeddedHA() throws Exception {
    PropertyKey key = PropertyKey.MASTER_JOURNAL_FLUSH_TIMEOUT_MS;
    Map<Integer, Map<PropertyKey, String>> masterProperties
        = generatePropertyWithDifferentValues(TEST_NUM_MASTERS, key);
    mCluster = MultiProcessCluster
        .newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_MASTERS_EMBEDDED_HA)
        .setClusterName("ConfigCheckerMultiMastersEmbeddedHATest")
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(0)
        .addProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.EMBEDDED.toString())
        .setMasterProperties(masterProperties)
        .build();
    mCluster.start();
    ConfigCheckReport report = getReport();
    // The master values of {@link PropertyKey#ALLUXIO_MASTER_JOURNAL_FOLDER} are different
    // when using embedded HA
    assertEquals(ConfigStatus.FAILED, report.getConfigStatus());
    assertThat(report.getConfigWarns().toString(),
        CoreMatchers.containsString(key.getName()));
    mCluster.notifySuccess();
  }

  @Test
  public void multiWorkers() throws Exception {
    PropertyKey key = PropertyKey.WORKER_FREE_SPACE_TIMEOUT;
    Map<Integer, Map<PropertyKey, String>> workerProperties
        = generatePropertyWithDifferentValues(TEST_NUM_WORKERS, key);
    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_WORKERS)
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setNumMasters(1)
        .setNumWorkers(TEST_NUM_WORKERS)
        .setWorkerProperties(workerProperties)
        .build();

    mCluster.start();
    ConfigCheckReport report = getReport();
    // The workers values of many directory related properties are different
    assertEquals(ConfigStatus.WARN, report.getConfigStatus());
    assertThat(report.getConfigWarns().toString(), CoreMatchers.containsString(key.getName()));
    mCluster.notifySuccess();
  }

  @Test
  public void multiNodes() throws Exception {
    PropertyKey key = PropertyKey.UNDERFS_LISTING_LENGTH;
    // Prepare properties
    Map<Integer, Map<PropertyKey, String>> properties = generatePropertyWithDifferentValues(
        TEST_NUM_MASTERS + TEST_NUM_WORKERS, key);
    Map<Integer, Map<PropertyKey, String>> masterProperties = properties.entrySet().stream()
        .filter(entry -> (entry.getKey() < TEST_NUM_MASTERS))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    Map<Integer, Map<PropertyKey, String>> workerProperties = properties.entrySet().stream()
        .filter(entry -> (entry.getKey() >= TEST_NUM_MASTERS))
        .collect(Collectors.toMap(entry -> entry.getKey() - TEST_NUM_MASTERS, Map.Entry::getValue));

    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_MULTI_NODES)
        .setClusterName("ConfigCheckerMultiNodesTest")
        .setNumMasters(TEST_NUM_MASTERS)
        .setNumWorkers(TEST_NUM_WORKERS)
        .setMasterProperties(masterProperties)
        .setWorkerProperties(workerProperties)
        .build();

    mCluster.start();
    ConfigCheckReport report = getReport();
    assertEquals(ConfigStatus.FAILED, report.getConfigStatus());
    assertThat(report.getConfigErrors().toString(), CoreMatchers.containsString(key.getName()));
    mCluster.notifySuccess();
  }

  @Test
  public void unsetVsSet() throws Exception {
    Map<Integer, Map<PropertyKey, String>> masterProperties = ImmutableMap.of(
        1, ImmutableMap.of(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION, "option"));

    mCluster = MultiProcessCluster.newBuilder(PortCoordination.CONFIG_CHECKER_UNSET_VS_SET)
        .setClusterName("ConfigCheckerUnsetVsSet")
        .setNumMasters(2)
        .setNumWorkers(0)
        .setMasterProperties(masterProperties)
        .build();
    mCluster.start();
    ConfigCheckReport report = getReport();
    Map<Scope, List<InconsistentProperty>> errors = report.getConfigErrors();
    assertTrue(errors.containsKey(Scope.MASTER));

    if (mCluster.getDeployMode().equals(DeployMode.ZOOKEEPER_HA)) {
      assertEquals(1, errors.get(Scope.MASTER).size());
      InconsistentProperty property = errors.get(Scope.MASTER).get(0);
      assertEquals(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION.getName(), property.getName());
      assertTrue(property.getValues().containsKey(Optional.of("option")));
      assertTrue(property.getValues().containsKey(Optional.empty()));
    } else {
      // When using embedded journal, the journal paths are different
      assertEquals(2, errors.get(Scope.MASTER).size());
      assertThat(report.getConfigErrors().toString(),
          CoreMatchers.containsString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION.getName()));
    }
    mCluster.notifySuccess();
  }

  private ConfigCheckReport getReport() throws Exception {
    mCluster.waitForAllNodesRegistered(WAIT_TIMEOUT_MS);
    MetaMasterClient client = mCluster.getMetaMasterClient();
    return client.getConfigReport();
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
      prop.put(key, ((ServerConfiguration.getMs(key) / Constants.SECOND_MS) + i) + "sec");
      properties.put(i, prop);
    }
    return properties;
  }
}
