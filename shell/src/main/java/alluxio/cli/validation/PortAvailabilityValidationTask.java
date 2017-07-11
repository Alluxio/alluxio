package alluxio.cli.validation;

import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Task for validating whether a specific port is available.
 */
public class PortAvailabilityValidationTask implements ValidationTask {
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
  public boolean validate() {
    if (Utils.isAlluxioRunning(mOwner)) {
      System.out.format("%s is already running. Skip validation.%n", mOwner);
      return true;
    }
    int port = NetworkAddressUtils.getPort(mServiceType);
    if (!isLocalPortAvailable(port)) {
      System.err.format("%s port %d is not available.%n", mServiceType.getServiceName(), port);
      return false;
    }
    return true;
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
