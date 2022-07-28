package alluxio.master.file.meta;

import alluxio.grpc.PathInvalidation;

import io.grpc.stub.StreamObserver;

/**
 * Stream for cross cluster invalidations.
 */
public class CrossClusterInvalidationStream {

  private final StreamObserver<PathInvalidation> mInvalidationStream;
  private boolean mCompleted = false;
  private final String mClusterId;

  /**
   * @param clusterId the cluster id
   * @param invalidationStream the invalidation stream
   */
  public CrossClusterInvalidationStream(
      String clusterId, StreamObserver<PathInvalidation> invalidationStream) {
    mInvalidationStream = invalidationStream;
    mClusterId = clusterId;
  }

  /**
   * @return the cluster id of this stream
   */
  public String getClusterId() {
    return mClusterId;
  }

  /**
   * @return the completed state of the stream
   */
  public boolean getCompleted() {
    return mCompleted;
  }

  /**
   * @param ufsPath publish a path
   * @return true if the stream is not completed, false otherwise
   */
  public synchronized boolean publishPath(String ufsPath) {
    if (mCompleted) {
      return false;
    }
    mInvalidationStream.onNext(PathInvalidation.newBuilder().setUfsPath(ufsPath).build());
    return true;
  }

  /**
   * Called on completion.
   */
  public synchronized void onCompleted() {
    mInvalidationStream.onCompleted();
    mCompleted = true;
  }

  /**
   * Called on error.
   * @param t the error
   */
  public synchronized void onError(Throwable t) {
    mInvalidationStream.onError(t);
    mCompleted = true;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CrossClusterInvalidationStream) {
      return ((CrossClusterInvalidationStream) o).mClusterId.equals(mClusterId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return mClusterId.hashCode();
  }
}
