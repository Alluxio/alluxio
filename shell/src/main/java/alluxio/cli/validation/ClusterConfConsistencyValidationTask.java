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

import javax.annotation.Nullable;

/**
 * Task for validating system limit for current user.
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
  public State validate(Map<String, String> optionMap) throws InterruptedException {
    Set<String> masters = ConfigurationUtils.getMasterHostnames(mConf);
    Set<String> workers = ConfigurationUtils.getWorkerHostnames(mConf);
    Set<String> nodes = Sets.union(masters, workers);
    Map<String, Properties> allProperties = new HashMap<>();
    Set<String> propertyNames = new HashSet<>();
    if (masters.isEmpty()) {
      System.err.println("No master nodes specified in conf/masters file");
      return State.SKIPPED;
    }
    if (workers.isEmpty()) {
      System.err.println("No worker nodes specified in conf/workers file");
      return State.SKIPPED;
    }
    State result = State.OK;
    for (String node : nodes) {
      Properties props = getNodeConf(node);
      if (props == null) {
        result = State.FAILED;
        continue;
      }
      allProperties.put(node, props);
      propertyNames.addAll(props.stringPropertyNames());
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
      State errLevel;
      switch (level) {
        case ENFORCE:
          errLabel = "Error";
          errLevel = State.FAILED;
          break;
        case WARN:
          errLabel = "Warning";
          errLevel = State.WARNING;
          break;
        default:
          System.err.format(
              "Error: Consistency check level \"%s\" for property \"%s\" is invalid.%n",
              level.name(), propertyName);
          result = State.FAILED;
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
          System.err.format("%s: Property \"%s\" is inconsistent between node %s and %s.%n",
              errLabel, propertyName, baseNode, remoteNode);
          System.err.format(" %s: %s%n %s: %s%n", baseNode, Objects.toString(baseValue, "not set"),
              remoteNode,  Objects.toString(remoteValue, "not set"));
          isConsistent = false;
        }
      }
      if (!isConsistent) {
        result = result == State.FAILED ? State.FAILED : errLevel;
      }
    }
    return result;
  }

  @Nullable
  private Properties getNodeConf(String node) {
    try {
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
    } catch (IOException e) {
      System.err.format("Unable to retrieve configuration for %s: %s.", node, e.getMessage());
      return null;
    }
  }
}
