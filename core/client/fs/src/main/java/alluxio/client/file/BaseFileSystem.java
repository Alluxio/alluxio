/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContextReinitializer.ReinitBlockerResource;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Bits;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UnmountPOptions;
import alluxio.master.MasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AclEntry;
import alluxio.uri.Authority;
import alluxio.util.FileSystemOptions;
import alluxio.wire.BlockLocation;
import alluxio.wire.BlockLocationInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

/**
* Default implementation of the {@link FileSystem} interface. Developers can extend this class
* instead of implementing the interface. This implementation reads and writes data through
* {@link FileInStream} and {@link FileOutStream}. This class is thread safe.
*/
@ThreadSafe
public class BaseFileSystem implements FileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BaseFileSystem.class);
  /** Used to manage closeable resources. */
  private final Closer mCloser = Closer.create();
  protected final FileSystemContext mFsContext;
  protected final AlluxioBlockStore mBlockStore;

  protected volatile boolean mClosed = false;

  /**
   * Constructs a new base file system.
   *
   * @param fsContext file system context
   */
  public BaseFileSystem(FileSystemContext fsContext) {
    mFsContext = fsContext;
    mBlockStore = AlluxioBlockStore.create(fsContext);
    mCloser.register(mFsContext);
  }

  /**
   * Shuts down the FileSystem. Closes all thread pools and resources used to perform operations. If
   * any operations are called after closing the context the behavior is undefined.
   *
   * @throws IOException
   */
  @Override
  public synchronized void close() throws IOException {
    // TODO(zac) Determine the behavior when closing the context during operations.
    if (!mClosed) {
      mClosed = true;
    }
    mCloser.close();
  }

  @Override
  public boolean isClosed() {
    // Doesn't require locking because mClosed is volatile and marked first upon close
    return mClosed;
  }

  @Override
  public void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      CheckAccessPOptions mergedOptions = FileSystemOptions
          .checkAccessDefaults(mFsContext.getPathConf(path))
          .toBuilder().mergeFrom(options).build();
      client.checkAccess(path, mergedOptions);
      LOG.debug("Checked access {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      CreateDirectoryPOptions mergedOptions = FileSystemOptions.createDirectoryDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      client.createDirectory(path, mergedOptions);
      LOG.debug("Created directory {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    checkUri(path);
    return rpc(client -> {
      CreateFilePOptions mergedOptions = FileSystemOptions.createFileDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      URIStatus status = client.createFile(path, mergedOptions);
      LOG.debug("Created file {}, options: {}", path.getPath(), mergedOptions);
      OutStreamOptions outStreamOptions =
          new OutStreamOptions(mergedOptions, mFsContext.getClientContext(),
              mFsContext.getPathConf(path));
      outStreamOptions.setUfsPath(status.getUfsPath());
      outStreamOptions.setMountId(status.getMountId());
      outStreamOptions.setAcl(status.getAcl());
      try {
        return new AlluxioFileOutStream(path, outStreamOptions, mFsContext);
      } catch (Exception e) {
        delete(path);
        throw e;
      }
    });
  }

  @Override
  public void delete(AlluxioURI path, DeletePOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      DeletePOptions mergedOptions = FileSystemOptions.deleteDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      client.delete(path, mergedOptions);
      LOG.debug("Deleted {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public boolean exists(AlluxioURI path, final ExistsPOptions options)
      throws IOException, AlluxioException {
    checkUri(path);
    try {
      return rpc(client -> {
        ExistsPOptions mergedOptions = FileSystemOptions.existsDefaults(
            mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
        // TODO(calvin): Make this more efficient
        client.getStatus(path, GrpcUtils.toGetStatusOptions(mergedOptions));
        return true;
      });
    } catch (FileDoesNotExistException | InvalidPathException e) {
      return false;
    }
  }

  @Override
  public void free(AlluxioURI path, final FreePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      FreePOptions mergedOptions = FileSystemOptions.freeDefaults(mFsContext.getPathConf(path))
          .toBuilder().mergeFrom(options).build();
      client.free(path, mergedOptions);
      LOG.debug("Freed {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public List<BlockLocationInfo> getBlockLocations(AlluxioURI path)
      throws IOException, AlluxioException {
    List<BlockLocationInfo> blockLocations = new ArrayList<>();
    // Don't need to checkUri here because we call other client operations
    List<FileBlockInfo> blocks = getStatus(path).getFileBlockInfos();
    for (FileBlockInfo fileBlockInfo : blocks) {
      // add the existing in-Alluxio block locations
      List<WorkerNetAddress> locations = fileBlockInfo.getBlockInfo().getLocations()
          .stream().map(BlockLocation::getWorkerAddress).collect(toList());
      if (locations.isEmpty()) { // No in-Alluxio location
        if (!fileBlockInfo.getUfsLocations().isEmpty()) {
          // Case 1: Fallback to use under file system locations with co-located workers.
          // This maps UFS locations to a worker which is co-located.
          Map<String, WorkerNetAddress> finalWorkerHosts = getHostWorkerMap();
          locations = fileBlockInfo.getUfsLocations().stream().map(
              location -> finalWorkerHosts.get(HostAndPort.fromString(location).getHost()))
              .filter(Objects::nonNull).collect(toList());
        }
        if (locations.isEmpty() && mFsContext.getPathConf(path)
            .getBoolean(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED)) {
          // Case 2: Fallback to add all workers to locations so some apps (Impala) won't panic.
          locations.addAll(getHostWorkerMap().values());
          Collections.shuffle(locations);
        }
      }
      blockLocations.add(new BlockLocationInfo(fileBlockInfo, locations));
    }
    return blockLocations;
  }

  private Map<String, WorkerNetAddress> getHostWorkerMap() throws IOException {
    List<BlockWorkerInfo> workers = mFsContext.getCachedWorkers();
    return workers.stream().collect(
        toMap(worker -> worker.getNetAddress().getHost(), BlockWorkerInfo::getNetAddress,
            (worker1, worker2) -> worker1));
  }

  @Override
  public AlluxioConfiguration getConf() {
    return mFsContext.getClusterConf();
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, final GetStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    URIStatus status = rpc(client -> {
      GetStatusPOptions mergedOptions = FileSystemOptions.getStatusDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      return client.getStatus(path, mergedOptions);
    });
    if (!status.isCompleted()) {
      LOG.warn("File {} is not yet completed. getStatus will see incomplete metadata.", path);
    }
    return status;
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, final ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    return rpc(client -> {
      // TODO(calvin): Fix the exception handling in the master
      ListStatusPOptions mergedOptions = FileSystemOptions.listStatusDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      return client.listStatus(path, mergedOptions);
    });
  }

  @Override
  public void iterateStatus(AlluxioURI path, final ListStatusPOptions options,
      Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      // TODO(calvin): Fix the exception handling in the master
      ListStatusPOptions mergedOptions = FileSystemOptions.listStatusDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      client.iterateStatus(path, mergedOptions, action);
      return null;
    });
  }

  @Override
  public void loadMetadata(AlluxioURI path, final ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      ListStatusPOptions mergedOptions = FileSystemOptions.listStatusDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options)
          .setLoadMetadataType(LoadMetadataPType.ALWAYS).setLoadMetadataOnly(true).build();
      client.listStatus(path, mergedOptions);
      return null;
    });
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, final MountPOptions options)
      throws IOException, AlluxioException {
    checkUri(alluxioPath);
    rpc(client -> {
      MountPOptions mergedOptions = FileSystemOptions.mountDefaults(
          mFsContext.getPathConf(alluxioPath)).toBuilder().mergeFrom(options).build();
      // TODO(calvin): Make this fail on the master side
      client.mount(alluxioPath, ufsPath, mergedOptions);
      LOG.debug("Mount {} to {}", ufsPath, alluxioPath.getPath());
      return null;
    });
  }

  @Override
  public void updateMount(AlluxioURI alluxioPath, final MountPOptions options)
      throws IOException, AlluxioException {
    checkUri(alluxioPath);
    rpc(client -> {
      MountPOptions mergedOptions = FileSystemOptions.mountDefaults(
          mFsContext.getPathConf(alluxioPath)).toBuilder().mergeFrom(options).build();
      client.updateMount(alluxioPath, mergedOptions);
      LOG.debug("UpdateMount on {}", alluxioPath.getPath());
      return null;
    });
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException {
    return rpc(FileSystemMasterClient::getMountTable);
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() throws IOException, AlluxioException {
    return rpc(FileSystemMasterClient::getSyncPathList);
  }

  @Override
  public void persist(final AlluxioURI path, final ScheduleAsyncPersistencePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      ScheduleAsyncPersistencePOptions mergedOptions =
          FileSystemOptions.scheduleAsyncPersistDefaults(mFsContext.getPathConf(path)).toBuilder()
              .mergeFrom(options).build();
      client.scheduleAsyncPersist(path, mergedOptions);
      LOG.debug("Scheduled persist for {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    checkUri(path);
    AlluxioConfiguration conf = mFsContext.getPathConf(path);
    URIStatus status = getStatus(path,
        FileSystemOptions.getStatusDefaults(conf).toBuilder()
            .setAccessMode(Bits.READ)
            .setUpdateTimestamps(options.getUpdateLastAccessTime())
            .build());
    return openFile(status, options);
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws FileDoesNotExistException, OpenDirectoryException, FileIncompleteException,
      IOException, AlluxioException {
    AlluxioURI path = new AlluxioURI(status.getPath());
    if (status.isFolder()) {
      throw new OpenDirectoryException(path);
    }
    if (!status.isCompleted()) {
      throw new FileIncompleteException(path);
    }
    AlluxioConfiguration conf = mFsContext.getPathConf(path);
    OpenFilePOptions mergedOptions = FileSystemOptions.openFileDefaults(conf)
        .toBuilder().mergeFrom(options).build();
    InStreamOptions inStreamOptions = new InStreamOptions(status, mergedOptions, conf);
    return new AlluxioFileInStream(status, inStreamOptions, mFsContext);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(src);
    checkUri(dst);
    rpc(client -> {
      RenamePOptions mergedOptions = FileSystemOptions.renameDefaults(mFsContext.getPathConf(dst))
          .toBuilder().mergeFrom(options).build();
      // TODO(calvin): Update this code on the master side.
      client.rename(src, dst, mergedOptions);
      LOG.debug("Renamed {} to {}, options: {}", src.getPath(), dst.getPath(), mergedOptions);
      return null;
    });
  }

  @Override
  public AlluxioURI reverseResolve(AlluxioURI ufsUri) throws IOException, AlluxioException {
    return rpc(client -> {
      AlluxioURI path = client.reverseResolve(ufsUri);
      LOG.debug("Reverse resolved {} to {}", ufsUri, path.getPath());
      return path;
    });
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclPOptions options) throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      SetAclPOptions mergedOptions = FileSystemOptions.setAclDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      client.setAcl(path, action, entries, mergedOptions);
      LOG.debug("Set ACL for {}, entries: {} options: {}", path.getPath(), entries,
          mergedOptions);
      return null;
    });
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    checkUri(path);
    SetAttributePOptions mergedOptions =
        FileSystemOptions.setAttributeClientDefaults(mFsContext.getPathConf(path))
            .toBuilder().mergeFrom(options).build();
    rpc(client -> {
      client.setAttribute(path, mergedOptions);
      LOG.debug("Set attributes for {}, options: {}", path.getPath(), options);
      return null;
    });
  }

  /**
   * Starts the active syncing process on an Alluxio path.
   *
   * @param path the path to sync
   */
  @Override
  public void startSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rpc(client -> {
      client.startSync(path);
      LOG.debug("Start syncing for {}", path.getPath());
      return null;
    });
  }

  /**
   * Stops the active syncing process on an Alluxio path.
   * @param path the path to stop syncing
   */
  @Override
  public void stopSync(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException {
    rpc(client -> {
      client.stopSync(path);
      LOG.debug("Stop syncing for {}", path.getPath());
      return null;
    });
  }

  @Override
  public void unmount(AlluxioURI path, UnmountPOptions options)
      throws IOException, AlluxioException {
    checkUri(path);
    rpc(client -> {
      UnmountPOptions mergedOptions = FileSystemOptions.unmountDefaults(
          mFsContext.getPathConf(path)).toBuilder().mergeFrom(options).build();
      client.unmount(path);
      LOG.debug("Unmounted {}, options: {}", path.getPath(), mergedOptions);
      return null;
    });
  }

  /**
   * Checks an {@link AlluxioURI} for scheme and authority information. Warn the user and throw an
   * exception if necessary.
   */
  protected void checkUri(AlluxioURI uri) {
    Preconditions.checkNotNull(uri, "uri");
    if (!mFsContext.getUriValidationEnabled()) {
      return;
    }

    if (uri.hasScheme()) {
      String warnMsg = "The URI scheme \"{}\" is ignored and not required in URIs passed to"
          + " the Alluxio Filesystem client.";
      switch (uri.getScheme()) {
        case Constants.SCHEME:
          LOG.warn(warnMsg, Constants.SCHEME);
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Scheme %s:// in AlluxioURI is invalid. Schemes in filesystem"
                  + " operations are ignored. \"alluxio://\" or no scheme at all is valid.",
                  uri.getScheme()));
      }
    }

    if (uri.hasAuthority()) {
      LOG.warn("The URI authority (hostname and port) is ignored and not required in URIs passed "
          + "to the Alluxio Filesystem client.");

      AlluxioConfiguration conf = mFsContext.getClusterConf();
      boolean skipAuthorityCheck = conf.isSet(PropertyKey.USER_SKIP_AUTHORITY_CHECK)
              && conf.getBoolean(PropertyKey.USER_SKIP_AUTHORITY_CHECK);
      if (!skipAuthorityCheck) {
        /* Even if we choose to log the warning, check if the Configuration host matches what the
         * user passes. If not, throw an exception letting the user know they don't match.
         */
        Authority configured =
                MasterInquireClient.Factory
                        .create(mFsContext.getClusterConf(),
                                mFsContext.getClientContext().getUserState())
                        .getConnectDetails().toAuthority();
        if (!configured.equals(uri.getAuthority())) {
          throw new IllegalArgumentException(
                  String.format("The URI authority %s does not match the configured value of %s.",
                          uri.getAuthority(), configured));
        }
      }
    }
  }

  @FunctionalInterface
  private interface RpcCallable<T, R> {
    R call(T t) throws IOException, AlluxioException;
  }

  /**
   * Sends an RPC to filesystem master.
   *
   * A resource is internally acquired to block FileSystemContext reinitialization before sending
   * the RPC.
   *
   * @param fn the RPC call
   * @param <R> the type of return value for the RPC
   * @return the RPC result
   */
  private <R> R rpc(RpcCallable<FileSystemMasterClient, R> fn)
      throws IOException, AlluxioException {
    try (ReinitBlockerResource r = mFsContext.blockReinit();
         CloseableResource<FileSystemMasterClient> client =
             mFsContext.acquireMasterClientResource()) {
      // Explicitly connect to trigger loading configuration from meta master.
      client.get().connect();
      return fn.call(client.get());
    } catch (NotFoundException e) {
      throw new FileDoesNotExistException(e.getMessage());
    } catch (AlreadyExistsException e) {
      throw new FileAlreadyExistsException(e.getMessage());
    } catch (InvalidArgumentException e) {
      throw new InvalidPathException(e.getMessage());
    } catch (FailedPreconditionException e) {
      // A little sketchy, but this should be the only case that throws FailedPrecondition.
      throw new DirectoryNotEmptyException(e.getMessage());
    } catch (UnavailableException e) {
      throw e;
    } catch (UnauthenticatedException e) {
      throw e;
    } catch (AlluxioStatusException e) {
      throw e.toAlluxioException();
    }
  }
}
