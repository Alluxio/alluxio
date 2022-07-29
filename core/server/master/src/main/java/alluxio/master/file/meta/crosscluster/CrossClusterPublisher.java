package alluxio.master.file.meta.crosscluster;

/**
 * Used to notify modifications to ufs paths using cross cluster sync.
 */
public interface CrossClusterPublisher {

  /**
   * Notify modification of path for cross cluster sync.
   * @param ufsPath the ufsPath
   */
  void publish(String ufsPath);
}
