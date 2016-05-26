/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.kafka.connect;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link AlluxioSinkConnector} class..
 */
public class AlluxioSinkConnectorTest {

  /**
   * Tests for setting up task configs.
   */
  @Test
  public void taskConfigsTest() {
    Map<String, String> configProperties = new HashMap<>();
    configProperties.put(AlluxioSinkConnectorConfig.ALLUXIO_URL, "alluxio://localhost:19998");
    configProperties.put(AlluxioSinkConnectorConfig.TOPICS_DIR, "topics");
    configProperties.put(AlluxioSinkConnectorConfig.ROTATION_RECORD_NUM, "3");

    AlluxioSinkConnector connector = new AlluxioSinkConnector();
    connector.start(configProperties);

    List<Map<String, String>> taskConfigs = connector.taskConfigs(5);
    Assert.assertEquals(taskConfigs.size(), 5);
  }
}
