package alluxio.master.cross.cluster;

import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.PathSubscription;
import alluxio.master.file.meta.cross.cluster.CrossClusterInvalidationStream;
import alluxio.master.file.meta.cross.cluster.MountSync;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for testing cross cluster invalidation subscriptions at masters.
 */
public class TestingFileSystemMasterClientServiceHandler
    extends FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceImplBase {

  private final List<CrossClusterInvalidationStream> mStreams = new ArrayList<>();

//  private final FileSystemMaster mFileSystemMaster;
//
//  /**
//   * @param fileSystemMaster the file system master
//   */
//  public TestingFileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster) {
//    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
//  }

  @Override
  public synchronized void subscribeInvalidations(
      PathSubscription pathSubscription, StreamObserver<PathInvalidation> stream) {
    mStreams.add(new CrossClusterInvalidationStream(MountSync.fromPathSubscription(
        pathSubscription), stream));
  }

  /**
   * @return the list of created streams
   */
  public synchronized List<CrossClusterInvalidationStream> getStreams() {
    return new ArrayList<>(mStreams);
  }
}
