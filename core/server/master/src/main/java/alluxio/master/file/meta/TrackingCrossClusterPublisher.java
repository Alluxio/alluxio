package alluxio.master.file.meta;

import alluxio.grpc.PathInvalidation;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;
import io.grpc.stub.StreamObserver;

/**
 * A testing implementation of {@link CrossClusterPublisher} that tracks paths published
 * in a list.
 */
public class TrackingCrossClusterPublisher {

  static class TrackingStream implements StreamObserver<PathInvalidation> {

    Collection<String> mPaths = new ConcurrentLinkedQueue<>();
    private boolean mCompleted = false;

    @Override
    public void onNext(PathInvalidation value) {
      if (mCompleted) {
        throw new RuntimeException("called onNext after completed");
      }
      mPaths.add(value.getUfsPath());
    }

    @Override
    public void onError(Throwable t) {
      throw new RuntimeException(t);
    }

    @Override
    public void onCompleted() {
      mCompleted = true;
    }
  }

  ConcurrentHashMap<String, TrackingStream> mPublishedPaths =
      new ConcurrentHashMap<>();

  /**
   * Tracks the published paths by cluster name for testing.
   */
  public TrackingCrossClusterPublisher() {
  }

  /**
   * Get the tracking stream for the given cluster id.
   * @param clusterId the cluster id
   * @return the tracking stream
   */
  public StreamObserver<PathInvalidation> getStream(String clusterId) {
    return mPublishedPaths.computeIfAbsent(clusterId, (key) -> new TrackingStream());
  }

  /**
   * @param clusterId the cluster id
   * @return the published paths for the cluster id
   */
  public Stream<String> getPublishedPaths(String clusterId) {
    return mPublishedPaths.getOrDefault(clusterId, new TrackingStream()).mPaths.stream();
  }
}
