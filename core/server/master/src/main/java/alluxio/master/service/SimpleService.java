package alluxio.master.service;

/**
 * Defines a simple start/promote/demote/stop interface for interacting with simple Alluxio
 * master components such as the web server, metrics server, rpc server, etc...
 */
public interface SimpleService {
  /**
   * Starts the service. Leaves the service in {@link alluxio.grpc.NodeState#STANDBY} mode.
   * Leaves the service in the same state as {@link #demote()}.
   */
  void start();

  /**
   * Promotes the service to act as in {@link alluxio.grpc.NodeState#PRIMARY}.
   */
  void promote();

  /**
   * Demotes the service back to {@link alluxio.grpc.NodeState#STANDBY}.
   */
  void demote();

  /**
   * Stops the service altogether and cleans up any state left.
   */
  void stop();
}
