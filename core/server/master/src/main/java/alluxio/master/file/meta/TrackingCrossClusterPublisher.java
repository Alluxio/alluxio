package alluxio.master.file.meta;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * A testing implementation of {@link CrossClusterPublisher} that tracks paths published
 * in a list.
 */
public class TrackingCrossClusterPublisher implements CrossClusterPublisher {

  CrossClusterIntersection mCrossClusterIntersection;
  ConcurrentHashMap<String, Collection<String>> mPublishedPaths =
      new ConcurrentHashMap<>();

  /**
   * Tracks the published paths by cluster name for testing.
   * @param crossClusterIntersection the cross cluster subscription paths
   */
  public TrackingCrossClusterPublisher(CrossClusterIntersection crossClusterIntersection) {
    mCrossClusterIntersection = crossClusterIntersection;
  }

  @Override
  public void publish(String ufsPath) {
    mCrossClusterIntersection.getClusters(ufsPath).forEach((cluster) -> {
      Collection<String> queue =
          mPublishedPaths.computeIfAbsent(cluster, (key) -> new ConcurrentLinkedQueue<>());
      queue.add(ufsPath);
    });
  }

  /**
   * @param clusterId the cluster id
   * @return the published paths for the cluster id
   */
  public Stream<String> getPublishedPaths(String clusterId) {
    return mPublishedPaths.getOrDefault(clusterId, Collections.emptyList()).stream();
  }
}
