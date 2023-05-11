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
import alluxio.util.ShellUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

/**
 * Task for validating whether a specific port is available.
 */
public final class PortAvailabilityValidationTask extends AbstractValidationTask {
  private final ServiceType mServiceType;
  private final String mOwner;
  private final AlluxioConfiguration mConf;

  /**
   * Creates a new instance of {@link PortAvailabilityValidationTask}.
   *
   * @param serviceType Alluxio network service whose port will be validated
   * @param owner Alluxio class name who owns the network service
   * @param conf configuration
   */
  public PortAvailabilityValidationTask(ServiceType serviceType, String owner,
                                        AlluxioConfiguration conf) {
    mServiceType = serviceType;
    mOwner = owner;
    mConf = conf;
  }

  @Override
  public String getName() {
    return "ValidateAlluxioPorts";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionsMap) {
    StringBuilder msg = new StringBuilder();
    StringBuilder advice = new StringBuilder();

    if (ShellUtils.isAlluxioRunning(mOwner)) {
      msg.append(String.format("%s is already running. Skip validation.%n", mOwner));
      return new ValidationTaskResult(ValidationUtils.State.SKIPPED, getName(),
              msg.toString(), advice.toString());
    }
    int port = NetworkAddressUtils.getPort(mServiceType, mConf);
    if (!isLocalPortAvailable(port)) {
      msg.append(String.format("%s port %d is not available.%n",
              mServiceType.getServiceName(), port));
      advice.append(String.format("Please open your port %s for service %s.%n",
              port, mServiceType.getServiceName()));
      return new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
              msg.toString(), advice.toString());
    }
    msg.append("All ports are validated.\n");
    return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
            msg.toString(), advice.toString());
  }

  private static boolean isLocalPortAvailable(int port) {
    try (ServerSocket socket = new ServerSocket(port)) {
      socket.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
