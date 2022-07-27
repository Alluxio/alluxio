package alluxio.master.file.meta;

import alluxio.grpc.PathInvalidation;
import io.grpc.stub.StreamObserver;

public class CrossClusterInvalidationStream {

  StreamObserver<PathInvalidation> mInvalidationStream;

  public CrossClusterInvalidationStream(StreamObserver<PathInvalidation> invalidationStream) {
    mInvalidationStream = invalidationStream;
  }

  public void publishPath(String ufsPath) {
    mInvalidationStream.onNext(PathInvalidation.newBuilder().setUfsPath(ufsPath).build());
  }

  public void onCompleted() {
    mInvalidationStream.onCompleted();
  }

  public void onError(Throwable t) {
    mInvalidationStream.onError(t);
  }
}
