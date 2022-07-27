package alluxio.master.file.meta;

/**
 * A Cross Cluster Publisher that only does no ops.
 */
public class NoOpCrossClusterPublisher implements CrossClusterPublisher {

  @Override
  public void publish(String ufsPath) {
    // noop
  }
}
