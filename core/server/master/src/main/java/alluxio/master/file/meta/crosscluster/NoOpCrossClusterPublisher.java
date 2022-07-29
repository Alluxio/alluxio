package alluxio.master.file.meta.crosscluster;

import alluxio.master.file.meta.crosscluster.CrossClusterPublisher;

/**
 * A Cross Cluster Publisher that only does no ops.
 */
public class NoOpCrossClusterPublisher implements CrossClusterPublisher {

  @Override
  public void publish(String ufsPath) {
    // noop
  }
}
