package alluxio.master.file.meta.crosscluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publisher for direct cluster to cluster subscriptions from other clusters.
 */
public class DirectCrossClusterPublisher implements CrossClusterPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(DirectCrossClusterPublisher.class);

  private final CrossClusterIntersection<CrossClusterInvalidationStream> mCrossClusterIntersection
      = new CrossClusterIntersection<>();
  private final ConcurrentHashMap<MountSync, CrossClusterInvalidationStream> mMountSyncToStream =
      new ConcurrentHashMap<>();

  /**
   * Create a direct cross cluster publisher.
   */
  public DirectCrossClusterPublisher() {}

  /**
   * Add a subscriber.
   * @param stream the stream to place invalidations on
   */
  public void addSubscriber(CrossClusterInvalidationStream stream) {
    LOG.info("Received a cross cluster subscription from {}", stream.getMountSync());
    mMountSyncToStream.compute(stream.getMountSync(), (key, prevStream) -> {
      if (prevStream != null) {
        LOG.info("Removing previous cross cluster subscription from {}", stream.getMountSync());
      }
      mCrossClusterIntersection.addMapping(stream.getMountSync().getClusterId(),
          stream.getMountSync().getUfsPath(), stream);
      stream.publishPath(stream.getMountSync().getUfsPath());
      return stream;
    });
  }

  @Override
  public void publish(String ufsPath) {
    mCrossClusterIntersection.getClusters(ufsPath).forEach((stream) -> {
      if (stream != null) {
        if (!stream.publishPath(ufsPath)) {
          LOG.info("Removing a cross cluster subscription from {}"
                  + " due to stream being completed", stream.getMountSync());
          mCrossClusterIntersection.removeMapping(stream.getMountSync().getClusterId(),
              stream.getMountSync().getUfsPath(),
              CrossClusterInvalidationStream::getCompleted);
        }
      }
    });
  }

  @Override
  public void close() throws IOException {
    mMountSyncToStream.entrySet().removeIf((entry) -> {
      entry.getValue().onCompleted();
      return true;
    });
  }
}
