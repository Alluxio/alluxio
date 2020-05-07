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

import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import java.util.Map;
import java.util.Set;

/**
 * Task for validating SSH reachability.
 */
public final class SshValidationTask extends AbstractValidationTask {
  /**
   * Creates a new instance of {@link SshValidationTask}.
   */
  public SshValidationTask() {
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    Set<String> nodes = ConfigurationUtils.getServerHostnames();
    if (nodes == null) {
      return TaskResult.FAILED;
    }

    boolean hasUnreachableNodes = false;
    for (String nodeName : nodes) {
      if (!CommonUtils.isAddressReachable(nodeName, 22)) {
        System.err.format("Unable to reach ssh port 22 on node %s.%n", nodeName);
        hasUnreachableNodes = true;
      }
    }
    return hasUnreachableNodes ? TaskResult.WARNING : TaskResult.OK;
  }
}
