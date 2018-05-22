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

import alluxio.PropertyKey;
import alluxio.multi.process.MultiProcessCluster;
import alluxio.testutils.BaseIntegrationTest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test server-side configuration checker.
 */
public class ConfigCheckerIntegrationTest extends BaseIntegrationTest {
  private static final int TEST_NUM_WORKERS = 2;

  @Test
  public void MultiWorkersWithDiffServerConf() throws Exception {
    Map<Integer, Map<PropertyKey, String>> workerProperties = generateProperties(TEST_NUM_WORKERS);
    MultiProcessCluster cluster = MultiProcessCluster.newBuilder()
        .setClusterName("ConfigCheckerMultiWorkersTest")
        .setNumWorkers(TEST_NUM_WORKERS)
        .setDeployMode(MultiProcessCluster.DeployMode.ZOOKEEPER_HA)
        .setWorkerProperties(workerProperties)
        .build();
    try {
      cluster.start();
      cluster.notifySuccess();
    } finally {
      cluster.destroy();
    }
  }

  /**
   * Generates a map that different nodes contain different values for one property.
   *
   * @param nodeNum the number of nodes to test
   * @return generated properties
   */
  private Map<Integer, Map<PropertyKey, String>> generateProperties(int nodeNum) {
    Map<Integer, Map<PropertyKey, String>> properties = new HashMap<>();
    for (int i = 0; i < nodeNum; i++) {
      Map<PropertyKey, String> prop = new HashMap<>();
      // Use as an example, will change to other sell generated property key
      prop.put(PropertyKey.MASTER_THRIFT_SHUTDOWN_TIMEOUT, (60 + i) + "sec");
      properties.put(i, prop);
    }
    return properties;
  }
}
