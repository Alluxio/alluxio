package alluxio.master.file.meta.cross.cluster;

import alluxio.ClientContext;
import alluxio.client.cross.cluster.CrossClusterClient;
import alluxio.client.cross.cluster.CrossClusterClientContextBuilder;
import alluxio.client.cross.cluster.RetryHandlingCrossClusterMasterClient;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Keeps the state of the cross cluster objects on the master.
 */
public class CrossClusterMasterState implements Closeable {

  final boolean mCrossClusterEnabled =
      Configuration.getBoolean(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE);

  /** Connection to the cross cluster configuration service. */
  final CrossClusterClient mCrossClusterClient = mCrossClusterEnabled
      ? new RetryHandlingCrossClusterMasterClient(new CrossClusterClientContextBuilder(
      ClientContext.create()).build()) : null;

  /** Used to publish modifications to paths using cross cluster sync. */
  final CrossClusterPublisher mCrossClusterPublisher = mCrossClusterEnabled
      ? new DirectCrossClusterPublisher() : new NoOpCrossClusterPublisher();

  final CrossClusterMountClientRunner mCrossClusterMountClientRunner = mCrossClusterEnabled
      ? new CrossClusterMountClientRunner(mCrossClusterClient) : null;

  /**
   * Starts the cross cluster services on the master.
   */
  public void start() {
    mCrossClusterMountClientRunner.start();
  }

  /**
   * @return the client for connecting to cross cluster configuration services
   */
  public Optional<CrossClusterClient> getCrossClusterClient() {
    return Optional.ofNullable(mCrossClusterClient);
  }

  /**
   * @return true if cross cluster synchronization is enabled
   */
  public boolean isCrossClusterEnabled() {
    return mCrossClusterEnabled;
  }

  /**
   * @return the cross cluster publisher
   */
  public CrossClusterPublisher getCrossClusterPublisher() {
    return mCrossClusterPublisher;
  }

  @Override
  public void close() throws IOException {
    if (mCrossClusterEnabled) {
      mCrossClusterClient.close();
      mCrossClusterPublisher.close();
      mCrossClusterMountClientRunner.close();
    }
  }
}
