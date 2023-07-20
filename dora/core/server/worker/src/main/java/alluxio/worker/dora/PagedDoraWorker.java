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

package alluxio.worker.dora;

import static alluxio.client.file.cache.CacheUsage.PartitionDescriptor.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.DefaultStorageTierAssoc;
import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.meta.DoraMeta;
import alluxio.resource.PooledResource;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsInputStreamCache;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.CommonUtils;
import alluxio.util.ModeUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.grpc.GrpcExecutors;
import alluxio.worker.task.CopyHandler;
import alluxio.worker.task.DeleteHandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.inject.Named;

/**
 * Page store based dora worker.
 */
public class PagedDoraWorker extends AbstractWorker implements DoraWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PagedDoraWorker.class);
  public static final long DUMMY_BLOCK_SIZE = 64L * 1024 * 1024;
  // for now Dora Worker does not support Alluxio <-> UFS mapping,
  // and assumes all UFS paths belong to the same UFS.
  private static final int MOUNT_POINT = 1;
  private final Closer mResourceCloser = Closer.create();
  private final AtomicReference<Long> mWorkerId;
  private final CacheManager mCacheManager;
  private final DoraUfsManager mUfsManager;
  private final DoraMetaManager mMetaManager;
  private final UfsInputStreamCache mUfsStreamCache;
  private final long mPageSize;
  private final AlluxioConfiguration mConf;
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final String mRootUFS;
  private FileSystemContext mFsContext;
  private MkdirsOptions mMkdirsRecursive;
  private MkdirsOptions mMkdirsNonRecursive;

  private WorkerNetAddress mAddress;

  private final UnderFileSystem mUfs;

  private final DoraOpenFileHandleContainer mOpenFileHandleContainer;

  private final boolean mClientWriteToUFSEnabled;

  /**
   * Constructor.
   *
   * @param workerId
   * @param conf
   * @param cacheManager
   */
  @Inject
  public PagedDoraWorker(
      @Named("workerId") AtomicReference<Long> workerId,
      AlluxioConfiguration conf,
      CacheManager cacheManager) {
    this(workerId, conf, cacheManager, new BlockMasterClientPool(),
        FileSystemContext.create(conf));
  }

  protected PagedDoraWorker(
      AtomicReference<Long> workerId,
      AlluxioConfiguration conf,
      CacheManager cacheManager,
      BlockMasterClientPool blockMasterClientPool,
      FileSystemContext fileSystemContext) {
    super(ExecutorServiceFactories.fixedThreadPool("dora-worker-executor", 5));
    mWorkerId = workerId;
    mConf = conf;
    mRootUFS = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    mUfsManager = mResourceCloser.register(new DoraUfsManager());
    mFsContext = mResourceCloser.register(fileSystemContext);
    mUfsStreamCache = new UfsInputStreamCache();
    mUfs = UnderFileSystem.Factory.create(
        mRootUFS,
        UnderFileSystemConfiguration.defaults(Configuration.global()));

    mPageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    mBlockMasterClientPool = blockMasterClientPool;
    mCacheManager = cacheManager;
    mMetaManager = mResourceCloser.register(
        new DoraMetaManager(this, mCacheManager, mUfs));
    mOpenFileHandleContainer = new DoraOpenFileHandleContainer();

    mMkdirsRecursive = MkdirsOptions.defaults(mConf).setCreateParent(true);
    mMkdirsNonRecursive = MkdirsOptions.defaults(mConf).setCreateParent(false);

    mClientWriteToUFSEnabled = Configuration.global()
        .getBoolean(PropertyKey.CLIENT_WRITE_TO_UFS_ENABLED);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return Collections.emptySet();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);
    mAddress = address;
    register();
    mOpenFileHandleContainer.start();

    // setup worker-master heartbeat
    // the heartbeat is only used to notify the aliveness of this worker, so that clients
    // can get the latest worker list from master.
    // TODO(bowen): once we set up a worker discovery service in place of master, remove this
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC,
            mResourceCloser.register(new BlockMasterSync()),
            () -> new FixedIntervalSupplier(Configuration.getMs(
                PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
            mConf, ServerUserState.global()));
  }

  private void register() throws IOException {
    Preconditions.checkState(mAddress != null, "worker not started");
    RetryPolicy retry = RetryUtils.defaultWorkerMasterClientRetry();
    while (true) {
      try (PooledResource<BlockMasterClient> bmc = mBlockMasterClientPool.acquireCloseable()) {
        mWorkerId.set(bmc.get().getId(mAddress));
        StorageTierAssoc storageTierAssoc =
            new DefaultStorageTierAssoc(ImmutableList.of(Constants.MEDIUM_MEM));
        bmc.get().register(
            mWorkerId.get(),
            storageTierAssoc.getOrderedStorageAliases(),
            ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.GB),
            ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            Configuration.getConfiguration(Scope.WORKER));
        LOG.info("Worker registered with worker ID: {}", mWorkerId.get());

        break;
      } catch (IOException ioe) {
        if (!retry.attempt()) {
          throw ioe;
        }
      }
    }
  }

  @Override
  public void stop() throws IOException {
    mOpenFileHandleContainer.shutdown();
    super.stop();
  }

  @Override
  public void close() throws IOException {
    try (AutoCloseable ignoredCloser = mResourceCloser;
         AutoCloseable ignoredCacheManager = mCacheManager
    ) {
      // do nothing as we are closing
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      super.close();
    }
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  @Override
  @Nullable
  public UfsStatus[] listStatus(String path, ListStatusPOptions options)
      throws IOException, AccessControlException {
    final long syncIntervalMs = options.hasCommonOptions()
        ? (options.getCommonOptions().hasSyncIntervalMs()
        ? options.getCommonOptions().getSyncIntervalMs() : -1) :
        -1;
    boolean isRecursive = options.getRecursive();
    final Optional<ListStatusResult> resultFromCache = mMetaManager.listCached(path, isRecursive);

    if (resultFromCache.isPresent()
        && (syncIntervalMs < 0
        || System.nanoTime() - resultFromCache.get().mTimeStamp
        <= syncIntervalMs * Constants.MS_NANO)) {
      return resultFromCache.get().mUfsStatuses;
    }
    mMetaManager.invalidateListingCache(path);
    Optional<UfsStatus[]> ufsStatuses =
        mMetaManager.listFromUfsThenCache(path, isRecursive);
    return ufsStatuses.orElse(null);
  }

  @Override
  public FileInfo getFileInfo(String ufsFullPath, GetStatusPOptions options)
      throws IOException, AccessControlException {
    long syncIntervalMs = options.hasCommonOptions()
        ? (options.getCommonOptions().hasSyncIntervalMs()
        ? options.getCommonOptions().getSyncIntervalMs() : -1) :
        -1;
    alluxio.grpc.FileInfo fi = getGrpcFileInfo(ufsFullPath, syncIntervalMs);
    int cachedPercentage = getCachedPercentage(fi, ufsFullPath);

    return GrpcUtils.fromProto(fi)
        .setInAlluxioPercentage(cachedPercentage)
        .setInMemoryPercentage(cachedPercentage);
  }

  protected alluxio.grpc.FileInfo getGrpcFileInfo(String ufsFullPath, long syncIntervalMs)
      throws IOException {
    Optional<DoraMeta.FileStatus> status = mMetaManager.getFromMetaStore(ufsFullPath);
    boolean shouldLoad = !status.isPresent();
    if (syncIntervalMs >= 0 && status.isPresent()) {
      // Check if the metadata is still valid.
      if (System.nanoTime() - status.get().getTs() > syncIntervalMs * Constants.MS_NANO) {
        shouldLoad = true;
      }
    }
    if (shouldLoad) {
      status = mMetaManager.loadFromUfs(ufsFullPath);
    }

    if (!status.isPresent()) {
      throw new FileNotFoundException("File " + ufsFullPath + " not found.");
    }
    return status.get().getFileInfo();
  }

  protected int getCachedPercentage(alluxio.grpc.FileInfo fi, String ufsFullPath) {
    // because cache manager uses hashed ufs path as file ID
    // TODO(bowen): we need a dedicated type for file IDs!
    String cacheManagerFileId = new AlluxioURI(ufsFullPath).hash();

    final long bytesInCache = mCacheManager.getUsage()
        .flatMap(usage -> usage.partitionedBy(file(cacheManagerFileId)))
        .map(CacheUsage::used).orElse(0L);
    final long fileLength = fi.getLength();
    final int cachedPercentage;
    if (fileLength > 0) {
      cachedPercentage = (int) (bytesInCache * 100 / fileLength);
    } else {
      cachedPercentage = 100;
    }
    return cachedPercentage;
  }

  /**
   * Build FileInfo from UfsStatus and UFS full Path.
   *
   * @param status
   * @param ufsFullPath
   * @return a FileInfo
   */
  public alluxio.grpc.FileInfo buildFileInfoFromUfsStatus(UfsStatus status, String ufsFullPath) {
    String filename = new AlluxioURI(ufsFullPath).getName();
    String relativePath = CommonUtils.stripPrefixIfPresent(ufsFullPath, mRootUFS);
    if (!relativePath.startsWith(AlluxioURI.SEPARATOR)) {
      relativePath = AlluxioURI.SEPARATOR + relativePath;
    }

    alluxio.grpc.FileInfo.Builder infoBuilder = alluxio.grpc.FileInfo.newBuilder()
        .setUfsType(mUfs.getUnderFSType())
        .setFileId(ufsFullPath.hashCode())
        .setName(filename)
        .setPath(relativePath)
        .setUfsPath(ufsFullPath)
        .setMode(status.getMode())
        .setFolder(status.isDirectory())
        .setOwner(status.getOwner())
        .setGroup(status.getGroup())
        .setCompleted(true)
        .setPersisted(true);
    if (status instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) status;
      infoBuilder.setLength(fileStatus.getContentLength())
          .setLastModificationTimeMs(status.getLastModifiedTime())
          .setBlockSizeBytes(fileStatus.getBlockSize());
      String contentHash = ((UfsFileStatus) status).getContentHash();
      if (contentHash != null) {
        infoBuilder.setContentHash(contentHash);
      }

      // get cached percentage
      String cacheManagerFileId = new AlluxioURI(ufsFullPath).hash();
      final long bytesInCache = mCacheManager.getUsage()
          .flatMap(usage -> usage.partitionedBy(file(cacheManagerFileId)))
          .map(CacheUsage::used).orElse(0L);
      final long fileLength = fileStatus.getContentLength();
      final int cachedPercentage;
      if (fileLength > 0) {
        cachedPercentage = (int) (bytesInCache * 100 / fileLength);
      } else {
        cachedPercentage = 100;
      }

      infoBuilder.setInAlluxioPercentage(cachedPercentage)
          .setInMemoryPercentage(cachedPercentage);
    }
    return infoBuilder.build();
  }

  /**
   * Build FileStatus from UfsStatus and UFS full Path.
   *
   * @param status the ufs status
   * @param ufsFullPath the full ufs path
   * @return the file status
   */
  public DoraMeta.FileStatus buildFileStatusFromUfsStatus(UfsStatus status, String ufsFullPath) {
    return DoraMeta.FileStatus.newBuilder()
        .setFileInfo(buildFileInfoFromUfsStatus(status, ufsFullPath))
        .setTs(System.nanoTime())
        .build();
  }

  @Override
  public BlockReader createFileReader(String fileId, long offset, boolean positionShort,
      Protocol.OpenUfsBlockOptions options) throws IOException, AccessControlException {
    UfsManager.UfsClient ufsClient;
    try {
      ufsClient = mUfsManager.get(MOUNT_POINT);
    } catch (NotFoundException e) {
      mUfsManager.addMount(MOUNT_POINT, new AlluxioURI(options.getUfsPath()),
          UnderFileSystemConfiguration.defaults(mConf));
      try {
        ufsClient = mUfsManager.get(MOUNT_POINT);
      } catch (NotFoundException e2) {
        throw new RuntimeException(
            String.format("Failed to get mount point for %s", options.getUfsPath()), e2);
      }
    }
    return PagedFileReader.create(mConf, mCacheManager, ufsClient, fileId,
        options.getUfsPath(), options.getBlockSize(), offset);
  }

  @Override
  public BlockWriter createFileWriter(String fileId, String ufsPath)
      throws AccessControlException, IOException {
    return new PagedFileWriter(this, ufsPath, mCacheManager, fileId, mPageSize);
  }

  @Override
  public ListenableFuture<List<LoadFileFailure>> load(
      boolean loadData, List<UfsStatus> ufsStatuses, UfsReadOptions options)
      throws AccessControlException, IOException {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    List<LoadFileFailure> errors = Collections.synchronizedList(new ArrayList<>());
    for (UfsStatus status : ufsStatuses) {
      String ufsFullPath = status.getUfsFullPath().toString();
      DoraMeta.FileStatus fs = buildFileStatusFromUfsStatus(status, ufsFullPath);
      mMetaManager.put(ufsFullPath, fs);
      // We use the ufs status sent from master to construct the file metadata,
      // and that ufs status might be stale.
      // This is a known consistency issue and will remain as long as the get metadata and
      // load data operations are not atomic.
      // Ideally, we can either:
      // 1. Use a single API to load the file alongside with fetching the file metadata
      // 2. Getting a last updated timestamp when loading data of a file and use it to
      //  validate the freshness of the metadata and discard the metadata if it is stale.
      // These two need UFS api support and cannot be achieved in a generic UFS interface.
      // We may be able to solve this by providing specific implementations for certain UFSes
      // in the future.
      if (loadData && status.isFile() && (status.asUfsFileStatus().getContentLength() > 0)) {
        try {
          ListenableFuture<Void> loadFuture = Futures.submit(() -> {
            try {
              if (options.hasUser()) {
                AuthenticatedClientUser.set(options.getUser());
              }
              loadData(status.getUfsFullPath().toString(), 0,
                  status.asUfsFileStatus().getContentLength());
            } catch (Throwable e) {
              LOG.error("Loading {} failed", status, e);
              boolean permissionCheckSucceeded = !(e instanceof AccessControlException);
              AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
              errors.add(LoadFileFailure.newBuilder().setUfsStatus(status.toProto())
                  .setCode(t.getStatus().getCode().value())
                  .setRetryable(t.isRetryable() && permissionCheckSucceeded)
                  .setMessage(t.getMessage()).build());
            }
          }, GrpcExecutors.BLOCK_READER_EXECUTOR);
          futures.add(loadFuture);
        } catch (RejectedExecutionException ex) {
          LOG.warn("BlockDataReaderExecutor overloaded.");
          AlluxioRuntimeException t = AlluxioRuntimeException.from(ex);
          errors.add(LoadFileFailure.newBuilder().setUfsStatus(status.toProto())
              .setCode(t.getStatus().getCode().value())
              .setRetryable(true)
              .setMessage(t.getMessage()).build());
        }
      }
    }
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.BLOCK_READER_EXECUTOR);
  }

  protected void loadData(String ufsPath, long mountId, long length)
      throws AccessControlException, IOException {
    Protocol.OpenUfsBlockOptions options =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(ufsPath).setMountId(mountId)
            .setNoCache(false).setOffsetInFile(0).setBlockSize(length)
            .build();
    String fileId = new AlluxioURI(ufsPath).hash();
    ByteBuf buf = PooledDirectNioByteBuf.allocate((int) (4 * mPageSize));
    try (BlockReader fileReader = createFileReader(fileId, 0, false, options)) {
      // cache file data
      while (fileReader.transferTo(buf) != -1) {
        buf.clear();
      }
    } catch (IOException | AccessControlException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public ListenableFuture<List<RouteFailure>> copy(List<Route> routes, UfsReadOptions readOptions,
                                                   WriteOptions writeOptions) {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    List<RouteFailure> errors = Collections.synchronizedList(new ArrayList<>());

    for (Route route : routes) {
      UnderFileSystem srcUfs = mUfsManager.getOrAdd(new AlluxioURI(route.getSrc()),
          UnderFileSystemConfiguration.defaults(mConf));
      UnderFileSystem dstUfs = mUfsManager.getOrAdd(new AlluxioURI(route.getDst()),
          UnderFileSystemConfiguration.defaults(mConf));
      String srcRoot = new AlluxioURI(route.getSrc()).getRootPath();
      String dstRoot = new AlluxioURI(route.getDst()).getRootPath();

      try (FileSystem srcFs = new UfsBaseFileSystem(mFsContext, new UfsFileSystemOptions(srcRoot),
          new UfsManager.UfsClient(() -> srcUfs, new AlluxioURI(srcRoot)));
          FileSystem dstFs = new UfsBaseFileSystem(mFsContext, new UfsFileSystemOptions(dstRoot),
              new UfsManager.UfsClient(() -> dstUfs, new AlluxioURI(dstRoot)))) {
        ListenableFuture<Void> future = Futures.submit(() -> {
          try {
            if (readOptions.hasUser()) {
              AuthenticatedClientUser.set(readOptions.getUser());
            }
            checkCopyPermission(route.getSrc(), route.getDst());
            CopyHandler.copy(route, writeOptions, srcFs, dstFs);
          } catch (Throwable t) {
            boolean permissionCheckSucceeded = !(t instanceof AccessControlException);
            LOG.error("Failed to copy {} to {}", route.getSrc(), route.getDst(), t);
            AlluxioRuntimeException e = AlluxioRuntimeException.from(t);
            RouteFailure.Builder builder =
                RouteFailure.newBuilder().setRoute(route).setCode(e.getStatus().getCode().value())
                    .setRetryable(e.isRetryable() && permissionCheckSucceeded);
            if (e.getMessage() != null) {
              builder.setMessage(e.getMessage());
            }
            errors.add(builder.build());
          }
        }, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
        futures.add(future);
      } catch (IOException e) {
        // ignore close error
      } catch (RejectedExecutionException e) {
        LOG.warn("BlockDataWriterExecutor overloaded.");
        AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
        RouteFailure.Builder builder =
            RouteFailure.newBuilder().setRoute(route).setCode(t.getStatus().getCode().value())
                .setRetryable(true);
        errors.add(builder.build());
      }
    }
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
  }

  protected UnderFileSystem getUnderFileSystem(String ufsPath) {
    return mUfsManager.getOrAdd(new AlluxioURI(ufsPath),
        UnderFileSystemConfiguration.defaults(mConf));
  }

  @Override
  public ListenableFuture<List<RouteFailure>> move(List<Route> routes, UfsReadOptions readOptions,
                                                   WriteOptions writeOptions) {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    List<RouteFailure> errors = Collections.synchronizedList(new ArrayList<>());
    for (Route route : routes) {
      UnderFileSystem srcUfs = getUnderFileSystem(route.getSrc());
      UnderFileSystem dstUfs = getUnderFileSystem(route.getDst());
      String srcRoot = new AlluxioURI(route.getSrc()).getRootPath();
      String dstRoot = new AlluxioURI(route.getDst()).getRootPath();
      try (FileSystem srcFs = new UfsBaseFileSystem(mFsContext, new UfsFileSystemOptions(srcRoot),
          new UfsManager.UfsClient(() -> srcUfs, new AlluxioURI(srcRoot)));
           FileSystem dstFs = new UfsBaseFileSystem(mFsContext, new UfsFileSystemOptions(dstRoot),
               new UfsManager.UfsClient(() -> dstUfs, new AlluxioURI(dstRoot)))) {
        ListenableFuture<Void> future = Futures.submit(() -> {
          Boolean deleteFailure = false;
          try {
            if (readOptions.hasUser()) {
              AuthenticatedClientUser.set(readOptions.getUser());
            }
            checkMovePermission(route.getSrc(), route.getDst());
            CopyHandler.copy(route, writeOptions, srcFs, dstFs);
            try {
              DeleteHandler.delete(new AlluxioURI(route.getSrc()), srcFs);
            } catch (Exception e) {
              deleteFailure = true;
              throw e;
            }
          } catch (Throwable t) {
            LOG.error("Failed to move {} to {}", route.getSrc(), route.getDst(), t);
            boolean permissionCheckSucceeded = !(t instanceof AccessControlException);
            AlluxioRuntimeException e = AlluxioRuntimeException.from(t);
            RouteFailure.Builder builder =
                RouteFailure.newBuilder().setRoute(route).setCode(e.getStatus().getCode().value())
                    .setRetryable(e.isRetryable() && permissionCheckSucceeded);
            if (e.getMessage() != null) {
              builder.setMessage(e.getMessage());
            }
            if (deleteFailure) {
              builder.setRetryable(false);
            }
            errors.add(builder.build());
          }
        }, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
        futures.add(future);
      } catch (IOException e) {
        // ignore close error
      } catch (RejectedExecutionException e) {
        LOG.warn("BlockDataWriterExecutor overloaded.");
        AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
        RouteFailure.Builder builder =
            RouteFailure.newBuilder().setRoute(route).setCode(t.getStatus().getCode().value())
                .setRetryable(true);
        errors.add(builder.build());
      }
    }
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.BLOCK_WRITER_EXECUTOR);
  }

  @Override
  public OpenFileHandle createFile(String path, CreateFilePOptions options)
      throws AccessControlException, IOException {
    // TODO(yuyang): Lock is needed.
    alluxio.grpc.FileInfo info;
    OpenFileHandle existingHandle = mOpenFileHandleContainer.find(path);
    if (existingHandle != null) {
      LOG.error("A file opened for write and not closed yet: path={} handle={}",
          path, existingHandle);
      // If want to enable this checking and throw exception, we need to handle such abnormal cases:
      // 1. If client disconnects without sending CompleteFile request, we must have a way to
      //    clean up the stale handle.
      // 2. some other abnormal case ...
      //throw new RuntimeException(new FileAlreadyExistsException("File is already opened"));
      mOpenFileHandleContainer.remove(path);
      existingHandle.close();
    }

    // construct open option based on @param options
    CreateOptions createOption = CreateOptions.defaults(mConf);
    if (options.hasMode()) {
      createOption.setMode(new Mode(ModeUtils.protoToShort(options.getMode())));
    }
    if (options.hasRecursive() && options.getRecursive()) {
      createOption.setCreateParent(true);
    }

    try {
      // Check if the target file already exists. If yes, return by throwing error.
      boolean overWrite = options.hasOverwrite() ? options.getOverwrite() : false;
      boolean exists = mUfs.exists(path);
      if (!overWrite && exists) {
        throw new RuntimeException(
            new FileAlreadyExistsException("File already exists but no overwrite flag"));
      } else if (overWrite) {
        // client is going to overwrite this file. We need to invalidate the cached meta and data.
        mMetaManager.removeFromMetaStore(path);
      }

      // Prepare a "fake" UfsStatus here. Please prepare more fields here.
      String owner = createOption.getOwner() != null ? createOption.getOwner() : "";
      String group = createOption.getGroup() != null ? createOption.getGroup() : "";
      UfsStatus status = new UfsFileStatus(new AlluxioURI(path).toString(),
                                "",
                                0,
                                CommonUtils.getCurrentMs(),
                                owner,
                                group,
                                createOption.getMode().toShort(),
                                DUMMY_BLOCK_SIZE);
      info = buildFileInfoFromUfsStatus(status, path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    OutputStream outStream;
    if (mClientWriteToUFSEnabled) {
      // client is writing directly to UFS. Worker does not write to UFS.
      outStream = null;
    } else {
      outStream = mUfs.create(path, createOption);
    }

    OpenFileHandle handle = new OpenFileHandle(path, info, options, outStream);
    //add to map.
    mOpenFileHandleContainer.add(path, handle);

    return handle;
  }

  @Override
  public void completeFile(String path, CompleteFilePOptions options, String uuid)
      throws IOException, AccessControlException {
    OpenFileHandle handle = mOpenFileHandleContainer.findAndVerify(path, uuid);
    if (handle != null) {
      mOpenFileHandleContainer.remove(path);
      handle.close();
      Optional<DoraMeta.FileStatus> status = mMetaManager.loadFromUfs(path);
      mMetaManager.invalidateListingCacheOfParent(path);
      if (!status.isPresent()) {
        throw new FileNotFoundException("Cannot retrieve file metadata of "
            + path + " when completing the file");
      }
    }
  }

  @Override
  public void delete(String path, DeletePOptions options) throws IOException,
      AccessControlException {
    try {
      mMetaManager.removeFromMetaStore(path);

      // TODO(hua) Close the open file handle?
      if (!options.getAlluxioOnly()) {
        // By being a cache, Dora assume the file exists in UFS when a delete is issued
        // So if the file does not exist in UFS, an IOException will be thrown here
        UfsStatus status = mUfs.getStatus(path);
        if (status.isFile()) {
          mUfs.deleteFile(path);
        } else {
          if (options.hasRecursive() && options.getRecursive()) {
            mUfs.deleteDirectory(path, DeleteOptions.RECURSIVE);
          } else {
            mUfs.deleteDirectory(path, DeleteOptions.NON_RECURSIVE);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rename(String src, String dst, RenamePOptions options)
      throws IOException, AccessControlException {
    try {
      UfsStatus status = mUfs.getStatus(src);
      if (status.isFile()) {
        mUfs.renameFile(src, dst);
      } else {
        mUfs.renameDirectory(src, dst);
      }
      mMetaManager.removeFromMetaStore(src);
      mMetaManager.loadFromUfs(dst);
      mMetaManager.invalidateListingCacheOfParent(dst);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void createDirectory(String path, CreateDirectoryPOptions options)
      throws IOException, AccessControlException {
    try {
      boolean success;
      if (options.hasRecursive() && options.getRecursive()) {
        success = mUfs.mkdirs(path, mMkdirsRecursive);
      } else {
        success = mUfs.mkdirs(path, mMkdirsNonRecursive);
      }
      mMetaManager.loadFromUfs(path);
      mMetaManager.invalidateListingCacheOfParent(path);
      if (!success) {
        throw new RuntimeException(
            new FileAlreadyExistsException(String.format("%s already exists", path)));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean exists(String path, ExistsPOptions options) throws IOException {
    long syncIntervalMs = options.hasCommonOptions()
        ? (options.getCommonOptions().hasSyncIntervalMs()
        ? options.getCommonOptions().getSyncIntervalMs() : -1) :
        -1;
    try {
      return getGrpcFileInfo(path, syncIntervalMs) != null;
    } catch (FileNotFoundException e) {
      return false;
    }
  }

  /**
   * Set attribute for this file/dir.
   * Please note, at this moment, the options::recursive is ignored. TODO(hua)
   */
  @Override
  public void setAttribute(String path, SetAttributePOptions options) throws IOException {
    if (options.hasPinned() || options.hasPersisted()
        || options.hasReplicationMax() || options.hasReplicationMin()
        || options.getXattrCount() != 0) {
      LOG.warn("UFS only supports setting mode, owner, and group. The other settings are "
              + "ignored (and no error is returned): {}",
          options);
    }

    if (options.hasMode()) {
      mUfs.setMode(path, ModeUtils.protoToShort(options.getMode()));
    }
    if (options.hasOwner() && options.hasGroup()) {
      mUfs.setOwner(path, options.getOwner(), options.getGroup());
    } else if (options.hasOwner()) {
      mUfs.setOwner(path, options.getOwner(), null);
    } else if (options.hasGroup()) {
      mUfs.setOwner(path, null, options.getGroup());
    }
    mMetaManager.loadFromUfs(path);
    mMetaManager.invalidateListingCacheOfParent(path);
  }

  @Override
  public void cleanupSession(long sessionId) {
  }

  private class BlockMasterSync implements HeartbeatExecutor {
    @Override
    public void heartbeat(long timeLimitMs) throws InterruptedException {
      final Command cmdFromMaster;
      try (PooledResource<BlockMasterClient> bmc = mBlockMasterClientPool.acquireCloseable()) {
        cmdFromMaster = bmc.get().heartbeat(mWorkerId.get(),
            ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.GB),
            ImmutableMap.of(Constants.MEDIUM_MEM, 0L),
            ImmutableList.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableList.of());
      } catch (IOException e) {
        LOG.warn("failed to heartbeat to master", e);
        return;
      }

      LOG.debug("received master command: {}", cmdFromMaster.getCommandType());
      // only handles re-register command
      if (cmdFromMaster.getCommandType() == CommandType.Register) {
        try {
          register();
        } catch (IOException e) {
          LOG.warn("failed to re-register to master during heartbeat", e);
        }
      }
    }

    @Override
    public void close() {
      // do nothing
    }
  }

  @VisibleForTesting
  UnderFileSystem getUfs() {
    return mUfs;
  }

  @VisibleForTesting
  DoraMetaManager getMetaManager() {
    return mMetaManager;
  }

  protected void checkCopyPermission(String srcPath, String dstPath)
      throws AccessControlException, IOException {
    // No-op
  }

  protected void checkMovePermission(String srcPath, String dstPath)
      throws AccessControlException, IOException {
    // No-op
  }

  protected DoraOpenFileHandleContainer getOpenFileHandleContainer() {
    return mOpenFileHandleContainer;
  }
}
