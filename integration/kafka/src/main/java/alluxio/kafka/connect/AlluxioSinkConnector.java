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

import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.connector.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is a Kafka Connect Connector implementation that exports data from Kafka to Alluxio.
 */
public final class AlluxioSinkConnector extends Connector {

  private Map<String, String> mConfigProperties;

  @Override
  public String version() {
    return AlluxioSinkConnectorConfig.ALLUXIO_CONNECTOR_VERSION;
  }

  @Override
  public void start(Map<String, String> map) {
    mConfigProperties = map;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return AlluxioSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> taskProps = new HashMap<>();
    taskProps.putAll(mConfigProperties);
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(taskProps);
    }
    return taskConfigs;
  }

  @Override
  public void stop() {
  }
}
