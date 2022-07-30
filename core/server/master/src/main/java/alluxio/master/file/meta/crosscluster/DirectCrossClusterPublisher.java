package alluxio.master.file.meta.crosscluster;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publisher for direct cluster to cluster connections.
 */
public class DirectCrossClusterPublisher implements CrossClusterPublisher {

  private final CrossClusterIntersection<CrossClusterInvalidationStream> mCrossClusterIntersection;
  private final ConcurrentHashMap<String, CrossClusterInvalidationStream> mClusterIdToStream =
      new ConcurrentHashMap<>();

  /**
   * Create a direct cross cluster publisher.
   * @param crossClusterIntersection the map of subscriptions
   */
  public DirectCrossClusterPublisher(
      CrossClusterIntersection<CrossClusterInvalidationStream> crossClusterIntersection) {
    mCrossClusterIntersection = crossClusterIntersection;
  }

  /**
   * add a subscriber
   * @param ufsPath the path to subscribe
   * @param stream the stream to place invalidations on
   */
  public void addSubscriber(String ufsPath, CrossClusterInvalidationStream stream) {
    mClusterIdToStream.compute(stream.getClusterId(), (key, prevStream) -> {
      if (prevStream != null) {
        prevStream.onCompleted();
      }
      mCrossClusterIntersection.addMapping(stream.getClusterId(), ufsPath, stream);
      stream.publishPath(ufsPath);
      return stream;
    });
  }

  @Override
  public void publish(String ufsPath) {
    mCrossClusterIntersection.getClusters(ufsPath).forEach((stream) -> {
      if (stream != null) {
        if (!stream.publishPath(ufsPath)) {
          mCrossClusterIntersection.removeMapping(stream.getClusterId(), ufsPath,
              CrossClusterInvalidationStream::getCompleted);
        }
      }
    });
  }
}
