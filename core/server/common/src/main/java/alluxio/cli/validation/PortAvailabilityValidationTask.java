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

  /**
   * Creates a new instance of {@link PortAvailabilityValidationTask}.
   *
   * @param serviceType Alluxio network service whose port will be validated
   * @param owner Alluxio class name who owns the network service
   */
  public PortAvailabilityValidationTask(ServiceType serviceType, String owner) {
    mServiceType = serviceType;
    mOwner = owner;
  }

  @Override
  public TaskResult validate(Map<String, String> optionsMap) {
    if (Utils.isAlluxioRunning(mOwner)) {
      System.out.format("%s is already running. Skip validation.%n", mOwner);
      return TaskResult.SKIPPED;
    }
    int port = NetworkAddressUtils.getPort(mServiceType);
    if (!isLocalPortAvailable(port)) {
      System.err.format("%s port %d is not available.%n", mServiceType.getServiceName(), port);
      return TaskResult.FAILED;
    }
    return TaskResult.OK;
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
