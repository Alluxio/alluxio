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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import java.util.Map;
import java.util.Set;

/**
 * Task for validating SSH reachability.
 */
public final class SshValidationTask extends AbstractValidationTask {
  private final AlluxioConfiguration mConf;

    /**
     * Creates a new instance of {@link SshValidationTask}
     * for validating ssh accessibility.
     * @param conf configuration
     */
  public SshValidationTask(AlluxioConfiguration conf) {
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateSshAccessibility";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    Set<String> nodes = ConfigurationUtils.getServerHostnames(mConf);
    if (nodes == null) {
      msg.append("Failed to find master/worker nodes from Alluxio configuration. ");
      advice.append(String.format("Please check your %s/master and %s/worker files. ",
              mConf.get(PropertyKey.CONF_DIR)));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }

    ValidationUtils.State state = ValidationUtils.State.OK;
    for (String nodeName : nodes) {
      if (!CommonUtils.isAddressReachable(nodeName, 22, 30 * Constants.SECOND_MS)) {
        msg.append(String.format("Unable to reach ssh port 22 on node %s.%n", nodeName));
        advice.append(String.format("Please configure password-less ssh to node %s.%n", nodeName));
        state = ValidationUtils.State.FAILED;
      }
    }
    return new ValidationTaskResult(state, getName(), msg.toString(), advice.toString());
  }
}
