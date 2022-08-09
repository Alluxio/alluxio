package alluxio.client.cross.cluster;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.PathSubscription;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * For testing the cross cluster file system client.
 */
public class TestingCrossClusterFileSystem implements FileSystemCrossCluster {

  private final FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceStub mClientAsync;

  /**
   * @param channel the connection channel
   */
  public TestingCrossClusterFileSystem(Channel channel) {
    mClientAsync = FileSystemMasterClientServiceGrpc.newStub(channel);
  }

  @Override
  public void subscribeInvalidations(String localClusterId, String ufsPath,
                                     StreamObserver<PathInvalidation> stream)
      throws IOException, AlluxioException {
    mClientAsync.subscribeInvalidations(PathSubscription.newBuilder().setClusterId(localClusterId)
            .setUfsPath(ufsPath).build(), stream);
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean exists(AlluxioURI path, ExistsPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void free(AlluxioURI path, FreePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioConfiguration getConf() {
    throw new UnsupportedOperationException();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
                            Consumer<? super URIStatus> action)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loadMetadata(AlluxioURI path, ListStatusPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, MountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, MountPointInfo> getMountTable()
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SyncPointInfo> getSyncPathList()
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void persist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
                     SetAclPOptions options) throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void startSync(AlluxioURI path) throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void stopSync(AlluxioURI path) throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    throw new UnsupportedOperationException();
  }
}
