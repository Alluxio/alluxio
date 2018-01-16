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

package alluxio.cli.validation;

import alluxio.Configuration;
import alluxio.PropertyKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Task for validating system limit for current user.
 */
public final class ClusterConfConsistencyValidationTask extends AbstractValidationTask {
  private static final Set<String> PROPERTIES_TO_IGNORE =  new HashSet<>(Arrays.asList(
      PropertyKey.USER_HOSTNAME.getName(),
      PropertyKey.WORKER_HOSTNAME.getName(),
      PropertyKey.MASTER_HOSTNAME.getName()
  ));

  @Override
  public TaskResult validate(Map<String, String> optionMap) throws InterruptedException {
    Set<String> nodes = new HashSet<>(Utils.readNodeList("masters"));
    nodes.addAll(Utils.readNodeList("workers"));
    Map<String, Properties> allProperties = new HashMap<>();
    Set<String> propertyNames = new HashSet<>();
    if (nodes.size() < 2) {
      System.err.println("Less than two nodes to check, skipping.");
      return TaskResult.SKIPPED;
    }
    boolean isConsistent = true;
    String baseNode = null;
    for (String node : nodes) {
      baseNode = node;
      Properties props = getNodeConf(node);
      if (props == null) {
        isConsistent = false;
        continue;
      }
      allProperties.put(node, props);
      propertyNames.addAll(props.stringPropertyNames());
    }
    Properties baselineProps = allProperties.get(baseNode);
    nodes.remove(baseNode);
    for (String propertyName : propertyNames) {
      if (PROPERTIES_TO_IGNORE.contains(propertyName)) {
        continue;
      }
      String baseValue = baselineProps.getProperty(propertyName);
      if (baseValue == null) {
        System.err.format("Property \"%s\" is absent on node %s.%n",
            propertyName, baseNode);
      }
      for (String remoteNode : nodes) {
        String remoteValue = allProperties.get(remoteNode).getProperty(propertyName);
        if (remoteValue == null) {
          System.err.format("Property \"%s\" is absent on node %s.%n", propertyName, remoteNode);
        } else if (baseValue != null && !remoteValue.equals(baseValue)) {
          System.err.format("Property \"%s\" is inconsistent on node %s and %s.%n",
              propertyName, baseNode, remoteNode);
          System.err.format("%s: %s%n%s: %s%n", baseNode, baseValue, remoteNode, remoteValue);
          isConsistent = false;
        }
      }
    }
    return isConsistent ? TaskResult.OK : TaskResult.WARNING;
  }

  private Properties getNodeConf(String node) {
    try {
      String homeDir = Configuration.get(PropertyKey.HOME);
      String remoteCommand = String.format(
          "%s/bin/alluxio getConf", homeDir);
      String localCommand = String.format(
          "ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no -tt %s \"bash %s\"",
          node, remoteCommand);
      String[] command = {"bash", "-c", localCommand};
      Properties properties = new Properties();
      Process process = Runtime.getRuntime().exec(command);
      properties.load(process.getInputStream());
      return properties;
    } catch (IOException e) {
      System.err.format("Unable retrieve configuration for %s: %s.", node, e.getMessage());
      return null;
    }
  }
}
