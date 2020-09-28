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

package alluxio.cli;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.Scope;
import alluxio.grpc.GrpcUtils;
import alluxio.util.ConfigurationUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * Task for validating Alluxio configuration consistency in the cluster.
 */
public final class ClusterConfConsistencyValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link ClusterConfConsistencyValidationTask}
   * for validating Alluxio configuration consistency in the cluster.
   * @param conf configuration
   */
  public ClusterConfConsistencyValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateClusterConfConsistency";
  }

  @Override
  protected ValidationTaskResult validateImpl(Map<String, String> optionMap)
          throws InterruptedException {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    Set<String> masters = ConfigurationUtils.getMasterHostnames(mConf);
    Set<String> workers = ConfigurationUtils.getWorkerHostnames(mConf);
    Set<String> nodes = Sets.union(masters, workers);
    Map<String, Properties> allProperties = new HashMap<>();
    Set<String> propertyNames = new HashSet<>();
    if (masters.isEmpty()) {
      msg.append(String.format("No master nodes specified in %s/masters file. ",
              mConf.get(PropertyKey.CONF_DIR)));
      advice.append(String.format("Please configure %s to contain the master node hostnames. ",
              mConf.get(PropertyKey.CONF_DIR)));
      return new ValidationTaskResult(ValidationUtils.State.WARNING, getName(),
              msg.toString(), advice.toString());
    }
    if (workers.isEmpty()) {
      msg.append(String.format("No worker nodes specified in %s/workers file. ",
              mConf.get(PropertyKey.CONF_DIR)));
      advice.append(String.format("Please configure %s to contain the worker node hostnames. ",
              mConf.get(PropertyKey.CONF_DIR)));
      return new ValidationTaskResult(ValidationUtils.State.WARNING, getName(),
              msg.toString(), advice.toString());
    }
    ValidationUtils.State state = ValidationUtils.State.OK;
    Exception ex = null;
    for (String node : nodes) {
      try {
        Properties props = getNodeConf(node);
        allProperties.put(node, props);
        propertyNames.addAll(props.stringPropertyNames());
      } catch (IOException e) {
        System.err.format("Unable to retrieve configuration for %s: %s.", node, e.getMessage());
        msg.append(String.format("Unable to retrieve configuration for %s: %s.",
                node, e.getMessage()));
        advice.append(String.format("Please check the connection from node %s. ", node));
        ex = e;
        state = ValidationUtils.State.FAILED;
        // Check all nodes before returning
        continue;
      }
    }
    for (String propertyName : propertyNames) {
      if (!PropertyKey.isValid(propertyName)) {
        continue;
      }
      PropertyKey propertyKey = PropertyKey.fromString(propertyName);
      PropertyKey.ConsistencyCheckLevel level = propertyKey.getConsistencyLevel();
      if (level == PropertyKey.ConsistencyCheckLevel.IGNORE) {
        continue;
      }
      Scope scope = propertyKey.getScope();
      Set<String> targetNodes = ImmutableSet.of();
      if (GrpcUtils.contains(scope, Scope.MASTER)) {
        targetNodes = masters;
      }
      if (GrpcUtils.contains(scope, Scope.WORKER)) {
        targetNodes = Sets.union(targetNodes, workers);
      }
      if (targetNodes.size() < 2) {
        continue;
      }
      String baseNode = null;
      String baseValue = null;
      boolean isConsistent = true;

      String errLabel;
      ValidationUtils.State errLevel;
      switch (level) {
        case ENFORCE:
          errLabel = "Error";
          errLevel = ValidationUtils.State.FAILED;
          break;
        case WARN:
          errLabel = "Warning";
          errLevel = ValidationUtils.State.WARNING;
          break;
        default:
          msg.append(String.format(
              "Error: Consistency check level \"%s\" for property \"%s\" is invalid.%n",
              level.name(), propertyName));
          advice.append(String.format("Please check property %s.%n", propertyName));
          state = ValidationUtils.State.FAILED;
          continue;
      }
      for (String remoteNode : targetNodes) {
        if (baseNode == null) {
          baseNode = remoteNode;
          Properties baselineProps = allProperties.get(baseNode);
          baseValue = baselineProps.getProperty(propertyName);
          continue;
        }
        String remoteValue = allProperties.get(remoteNode).getProperty(propertyName);
        if (!StringUtils.equals(remoteValue, baseValue)) {
          msg.append(String.format("%s: Property \"%s\" is inconsistent between node %s and %s.%n",
              errLabel, propertyName, baseNode, remoteNode));
          msg.append(String.format(" %s: %s%n %s: %s%n", baseNode,
                  Objects.toString(baseValue, "not set"),
              remoteNode,  Objects.toString(remoteValue, "not set")));
          advice.append(String.format("Please check your settings for property %s on %s and %s.%n",
                  propertyName, baseNode, remoteNode));
          isConsistent = false;
        }
      }
      if (!isConsistent) {
        state = state == ValidationUtils.State.FAILED ? ValidationUtils.State.FAILED : errLevel;
      }
    }
    return new ValidationTaskResult(state, getName(), msg.toString(), advice.toString());
  }

  private Properties getNodeConf(String node) throws IOException {
    String homeDir = mConf.get(PropertyKey.HOME);
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
  }
}
