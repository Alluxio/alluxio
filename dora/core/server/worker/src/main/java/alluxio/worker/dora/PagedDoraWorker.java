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
import alluxio.PositionReader;
import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.dora.netty.NettyDataReader;
import alluxio.client.file.options.UfsFileSystemOptions;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.collections.ConcurrentHashSet;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.AlreadyExistsRuntimeException;
import alluxio.exception.runtime.FailedPreconditionRuntimeException;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.FailedPreconditionException;
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
import alluxio.grpc.LoadDataSubTask;
import alluxio.grpc.LoadFailure;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.LoadMetadataSubTask;
import alluxio.grpc.LoadSubTask;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TaskStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.membership.MasterMembershipManager;
import alluxio.membership.MembershipManager;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
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
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.grpc.GrpcExecutors;
import alluxio.worker.task.CopyHandler;
import alluxio.worker.task.DeleteHandler;
import alluxio.worker.task.ValidateHandler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
  protected final Closer mResourceCloser = Closer.create();
  // TODO(lucy) change to string typed once membership manager got enabled by default
  private final AtomicReference<WorkerIdentity> mWorkerId;
  protected final CacheManager mCacheManager;
  protected final UfsManager mUfsManager;
  protected final DoraMetaManager mMetaManager;
  private final MembershipManager mMembershipManager;
  private final UfsInputStreamCache mUfsStreamCache;
  private final long mPageSize;
  protected final AlluxioConfiguration mConf;
  private final BlockMasterClientPool mBlockMasterClientPool;
  protected final FileSystemContext mFsContext;
  private MkdirsOptions mMkdirsRecursive;
  private MkdirsOptions mMkdirsNonRecursive;

  private WorkerNetAddress mAddress;
  private final DoraOpenFileHandleContainer mOpenFileHandleContainer;

  private final boolean mClientWriteToUFSEnabled;
  private final boolean mXAttrWriteToUFSEnabled;
  private final ConcurrentHashSet<PageId> mLoadingPages = new ConcurrentHashSet<>();
  private final ExecutorService mCacheDataExecutor = Executors.newFixedThreadPool(
      Configuration.getInt(PropertyKey.WORKER_PRELOAD_DATA_THREAD_POOL_SIZE));
  private final boolean mFastDataLoadEnabled;

  /**
   * Constructor.
   *
   * @param workerId
   * @param conf
   * @param cacheManager
   * @param membershipManager
   * @param blockMasterClientPool
   * @param ufsManager
   * @param metaManager
   * @param fileSystemContext
   */
  @Inject
  public PagedDoraWorker(
      @Named("workerId") AtomicReference<WorkerIdentity> workerId,
      AlluxioConfiguration conf,
      CacheManager cacheManager,
      MembershipManager membershipManager,
      BlockMasterClientPool blockMasterClientPool,
      UfsManager ufsManager,
      DoraMetaManager metaManager,
      FileSystemContext fileSystemContext
  ) {
    super(ExecutorServiceFactories.fixedThreadPool("dora-worker-executor", 5));
    mWorkerId = workerId;
    mConf = conf;
    mUfsManager = mResourceCloser.register(ufsManager);
    String rootUFS = mConf.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    mUfsManager.getOrAdd(new AlluxioURI(rootUFS),
        () -> UnderFileSystemConfiguration.defaults(mConf));
    mFsContext = mResourceCloser.register(fileSystemContext);
    mUfsStreamCache = new UfsInputStreamCache();
    mPageSize = mConf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    mBlockMasterClientPool = blockMasterClientPool;
    mCacheManager = cacheManager;
    mMetaManager = mResourceCloser.register(metaManager);
    mMembershipManager = membershipManager;
    mOpenFileHandleContainer = new DoraOpenFileHandleContainer();
    mMkdirsRecursive = MkdirsOptions.defaults(mConf).setCreateParent(true);
    mMkdirsNonRecursive = MkdirsOptions.defaults(mConf).setCreateParent(false);
    mClientWriteToUFSEnabled = mConf
        .getBoolean(PropertyKey.CLIENT_WRITE_TO_UFS_ENABLED);
    mXAttrWriteToUFSEnabled = mConf.getBoolean(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED);
    mFastDataLoadEnabled = mConf.getBoolean(PropertyKey.WORKER_FAST_DATA_LOAD_ENABLED);
  }

  /**
   * Finds the matching UFS instance for a UFS path.
   *
   * @param ufsUriStr the UFS path to find a UFS instance for
   * @return the corresponding UFS instance
   */
  @VisibleForTesting
  public UnderFileSystem getUfsInstance(String ufsUriStr) {
    AlluxioURI ufsUriUri = new AlluxioURI(ufsUriStr);
    try {
      Optional<UnderFileSystem> ufs = mUfsManager.get(ufsUriUri);
      return ufs.orElseThrow(() ->
          new IllegalArgumentException(String.format("UFS not registered for %s", ufsUriUri)));
    } catch (Exception e) {
      LOG.debug("failed to get UFS instance for URI {}", ufsUriStr, e);
      throw e;
    }
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
    // TODO(lucy): fallback to original logic using master for registration
    if (mMembershipManager instanceof MasterMembershipManager) {
      LOG.info("Using Master for heartbeating..");
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC,
              mResourceCloser.register(new BlockMasterSync()),
              () -> new FixedIntervalSupplier(Configuration.getMs(
                  PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
              mConf, ServerUserState.global()));
    }
  }

  /**
   * Register to join to the distributed membership.
   * @throws IOException
   */
  private void register() throws IOException {
    Preconditions.checkNotNull(mAddress, "worker not started");
    RetryPolicy retry = RetryUtils.defaultWorkerMasterClientRetry();
    // For regression purpose, use the original way of regsiter
    if (mMembershipManager instanceof MasterMembershipManager) {
      registerToMaster();
      return;
    }
    while (true) {
      try {
        LOG.info("{} membership manager starts joining...",
            mConf.get(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE));
        mMembershipManager.join(new WorkerInfo().setIdentity(mWorkerId.get()).setAddress(mAddress));
        break;
      } catch (IOException | UnavailableRuntimeException e) {
        /* Retry everything it might have coming from membership, as now a different worker
         * instance might assume same worker id in k8s pod restart situation. There might
         * be gaps in updating etcd states in the interim of transition.
         */
        LOG.error("Exception in join membership:", e);
        if (!retry.attempt()) {
          throw e;
        }
      }
    }
  }

  private void decommission() {
    // TO BE IMPLEMENTED
  }

  private void registerToMaster() throws IOException {
    Preconditions.checkNotNull(mAddress, "worker not started");
    RetryPolicy retry = RetryUtils.defaultWorkerMasterClientRetry();
    while (true) {
      try (PooledResource<BlockMasterClient> bmc = mBlockMasterClientPool.acquireCloseable()) {
        long id = bmc.get().getId(mAddress);
        LOG.debug("Obtained worker id {} from master", id);
        WorkerIdentity identity = WorkerIdentity.ParserV0.INSTANCE.fromLong(id);
        mWorkerId.set(identity);
        StorageTierAssoc storageTierAssoc =
            new DefaultStorageTierAssoc(ImmutableList.of(Constants.MEDIUM_MEM));
        bmc.get().register(
            id,
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
         AutoCloseable ignoredCacheManager = mCacheManager;
         AutoCloseable ignoredMembershipManager = mMembershipManager;
    ) {
      // do nothing as we are closing
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      super.close();
    }
  }

  @Override
  public AtomicReference<WorkerIdentity> getWorkerId() {
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
        && options.getLoadMetadataType() != LoadMetadataPType.ALWAYS
        && (syncIntervalMs < 0
        || System.nanoTime() - resultFromCache.get().mTimeStamp
        <= syncIntervalMs * Constants.MS_NANO)) {
      MetricsSystem.counter(MetricKey.WORKER_LIST_STATUS_HIT_REQUESTS.getName()).inc();
      return resultFromCache.get().mUfsStatuses;
    }
    mMetaManager.invalidateListingCache(path);
    Optional<UfsStatus[]> ufsStatuses =
        mMetaManager.listFromUfsThenCache(path, isRecursive);
    MetricsSystem.counter(MetricKey.WORKER_LIST_STATUS_EXTERNAL_REQUESTS.getName()).inc();
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
      // Checks if this file is under create/write.
      OpenFileHandle handle = mOpenFileHandleContainer.find(ufsFullPath);
      if (handle != null) {
        // The target is being written to. It's fine to return FileNotFound.
        LOG.debug("File {} is being written to.", ufsFullPath);
        status = Optional.empty();
      } else {
        status = mMetaManager.loadFromUfs(ufsFullPath);
        MetricsSystem.counter(MetricKey.WORKER_GET_FILE_INFO_EXTERNAL_REQUESTS.getName()).inc();
      }
    } else {
      MetricsSystem.counter(MetricKey.WORKER_GET_FILE_INFO_HIT_REQUESTS.getName()).inc();
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

  @Override
  public BlockReader createFileReader(String fileId, long offset, boolean positionShort,
      Protocol.OpenUfsBlockOptions options) throws IOException, AccessControlException {
    UnderFileSystem ufs = getUfsInstance(options.getUfsPath());
    return PagedFileReader.create(mConf, mCacheManager, ufs, fileId,
        options.getUfsPath(), options.getBlockSize(), offset);
  }

  @Override
  public BlockWriter createFileWriter(String fileId, String ufsPath)
      throws AccessControlException, IOException {
    return new PagedFileWriter(this, ufsPath, mCacheManager, fileId, mPageSize);
  }

  private boolean isAllPageCached(alluxio.grpc.UfsStatus status, long offset, long length) {
    String fileId = new AlluxioURI(status.getUfsFullPath()).hash();
    List<PageId> cachedPages = mCacheManager.getCachedPageIdsByFileId(fileId,
        status.getUfsFileStatus().getContentLength());
    // Ceiling
    int numOfPagesInBlock = (int) ((length + mPageSize - 1) / mPageSize);
    for (long pageIndex = offset / mPageSize; pageIndex < numOfPagesInBlock;
         pageIndex++) {
      PageId pageId = new PageId(fileId, pageIndex);
      if (!cachedPages.contains(pageId)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public ListenableFuture<LoadFileResponse> load(List<LoadSubTask> subTasks, boolean skipIfExists,
      UfsReadOptions options) throws AccessControlException, IOException {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    List<LoadFailure> errors = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger numSkipped = new AtomicInteger();
    AtomicLong skippedLength = new AtomicLong();
    for (LoadSubTask task : subTasks) {
      if (task.hasLoadMetadataSubtask()) {
        UfsStatus status = UfsStatus.fromProto(task.getLoadMetadataSubtask().getUfsStatus());
        loadMetadata(status, errors);
      }
      if (task.hasLoadDataSubtask()) {
        LoadDataSubTask subtask = task.getLoadDataSubtask();
        if (subtask.getLength() <= 0) {
          continue;
        }
        boolean countAsSkipped = skipIfExists && isAllPageCached(subtask.getUfsStatus(),
            subtask.getOffsetInFile(), subtask.getLength());
        if (countAsSkipped) {
          numSkipped.incrementAndGet();
          skippedLength.addAndGet(subtask.getLength());
          continue;
        }
        try {
          ListenableFuture<Void> loadFuture = submitLoadDataSubTask(subtask, options, errors);
          futures.add(loadFuture);
        } catch (RejectedExecutionException ex) {
          LOG.warn("Load task overloaded.");
          errors.add(LoadFailure.newBuilder().setSubtask(
                                    LoadSubTask.newBuilder().setLoadDataSubtask(subtask).build())
                                .setCode(Status.RESOURCE_EXHAUSTED.getCode().value())
                                .setRetryable(true).setMessage(ex.getMessage()).build());
        }
      }
    }
    return Futures.whenAllComplete(futures).call(
        () -> LoadFileResponse.newBuilder().addAllFailures(errors)
            .setBytesSkipped(skippedLength.get()).setNumSkipped(numSkipped.get())
            // Status is a required field, put it as a placeholder
            .setStatus(TaskStatus.SUCCESS).build(),
        GrpcExecutors.READER_EXECUTOR);
  }

  @Override
  // TODO(yimin) integrate this method with load() method
  public void cacheData(String ufsPath, long length, long pos, boolean isAsync)
      throws IOException {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    // TODO(yimin) To implement the sync data caching.
    alluxio.grpc.FileInfo fi = getGrpcFileInfo(ufsPath, -1);
    String fileId = new AlluxioURI(ufsPath).hash();
    for (long i = pos / mPageSize;
         i <= Math.min(pos + length, fi.getLength()) / mPageSize; ++i) {
      PageId pageId = new PageId(fileId, i);
      // TODO(yimin) As an optimization, data does not need to load on a page basis.
      // Can implement a bulk load mechanism and load a couple of pages at the same time,
      // to improve the performance.
      if (mCacheManager.hasPageUnsafe(pageId)) {
        continue;
      }
      long loadPos = i * mPageSize;
      long loadLength = Math.min(mPageSize, fi.getLength() - loadPos);
      if (loadLength == 0) {
        continue;
      }
      if (!mLoadingPages.addIfAbsent(pageId)) {
        continue;
      }

      futures.add(CompletableFuture.runAsync(() -> {
        try {
          if (mCacheManager.hasPageUnsafe(pageId)) {
            return;
          }
          LOG.debug("Preloading {} pos: {} length: {} started", ufsPath, loadPos, loadLength);
          loadPages(ufsPath, Collections.singletonList(pageId), fi.getLength());
          LOG.debug("Preloading {} pos: {} length: {} finished", ufsPath, loadPos, loadLength);
        } catch (Exception e) {
          LOG.info("Preloading failed for {} page: {}", ufsPath, pageId, e);
        } finally {
          mLoadingPages.remove(pageId);
        }
      }, mCacheDataExecutor));
      if (!isAsync) {
        try {
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private ListenableFuture<Void> submitLoadDataSubTask(
      LoadDataSubTask subTask, UfsReadOptions options, List<LoadFailure> errors) {
    ListenableFuture<Void> future =
        Futures.submit(() -> {
          try {
            if (options.hasUser()) {
              AuthenticatedClientUser.set(options.getUser());
            }
            long fileLength = subTask.getUfsStatus().getUfsFileStatus().getContentLength();
            if (subTask.hasMainWorker()) {
              WorkerNetAddress address = GrpcUtils.fromProto(subTask.getMainWorker());
              if (mAddress != address) {
                long chunkSize = mPageSize;
                Protocol.OpenUfsBlockOptions openOptions =
                    Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(subTask.getUfsPath())
                                                .setMountId(0).setNoCache(false)
                                                .setOffsetInFile(0)
                                                .setBlockSize(fileLength).build();
                Protocol.ReadRequest.Builder builder =
                    Protocol.ReadRequest.newBuilder().setBlockId(-1)
                                        .setOpenUfsBlockOptions(openOptions)
                                        .setChunkSize(chunkSize);
                try (PositionReader reader = new NettyDataReader(mFsContext, address, builder)) {
                  loadDataFromRemote(subTask.getUfsPath(), subTask.getOffsetInFile(),
                      subTask.getLength(), reader, (int) chunkSize);
                }
              }
            }
            else {
              if (mFastDataLoadEnabled) {
                loadPages(
                    subTask.getUfsPath(), 0, subTask.getOffsetInFile(), subTask.getLength(),
                    fileLength);
              } else {
                loadData(
                    subTask.getUfsPath(), 0, subTask.getOffsetInFile(), subTask.getLength(),
                    fileLength);
              }
            }
          } catch (Throwable e) {
            LOG.error("Loading {} failed", subTask, e);
            boolean permissionCheckSucceeded = !(e instanceof AccessControlException);
            AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
            errors.add(LoadFailure.newBuilder().setSubtask(
                                      LoadSubTask.newBuilder().setLoadDataSubtask(subTask).build())
                                  .setCode(t.getStatus().getCode().value())
                                  .setRetryable(permissionCheckSucceeded).setMessage(t.getMessage())
                                  .build());
          }
        }, GrpcExecutors.READER_EXECUTOR);
    return future;
  }

  /**
   * We use the ufs status sent from master to construct the file metadata,
   * and that ufs status might be stale.
   * This is a known consistency issue and will remain as long as the get metadata and
   * load data operations are not atomic.
   * Ideally, we can either:
   * 1. Use a single API to load the file alongside with fetching the file metadata
   * 2. Getting a last updated timestamp when loading data of a file and use it to
   * validate the freshness of the metadata and discard the metadata if it is stale.
   * These two need UFS api support and cannot be achieved in a generic UFS interface.
   * We may be able to solve this by providing specific implementations for certain UFSes
   * in the future.
   *
   * @param status the ufs status
   * @param errors the errors
   */
  private void loadMetadata(UfsStatus status, List<LoadFailure> errors) {
    String ufsFullPath = status.getUfsFullPath().toString();
    Map<String, String> xattrMap = null;
    UnderFileSystem ufs = getUfsInstance(ufsFullPath);
    try {
      if (mXAttrWriteToUFSEnabled) {
        xattrMap = ufs.getAttributes(ufsFullPath);
      }
      DoraMeta.FileStatus fs = buildFileStatusFromUfsStatus(getCacheUsage(), ufs.getUnderFSType(),
          status, ufsFullPath, xattrMap);
      mMetaManager.put(ufsFullPath, fs);
    } catch (Exception e) {
      LOG.error("Failed to put file status to meta manager", e);
      AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
      errors.add(LoadFailure.newBuilder().setSubtask(LoadSubTask.newBuilder()
                            .setLoadMetadataSubtask(
                                LoadMetadataSubTask.newBuilder().setUfsStatus(status.toProto())
                                                   .build()))
                            .setCode(t.getStatus().getCode().value()).setRetryable(true)
                            .setMessage(t.getMessage()).build());
    }
  }

  private void loadPages(String ufsPath, List<PageId> pageIds, long fileLength)
      throws AccessControlException, IOException {
    Optional<UnderFileSystem> ufs = mUfsManager.get(new AlluxioURI(ufsPath));
    if (!ufs.isPresent()) {
      throw new RuntimeException("Ufs not found for " + ufsPath);
    }
    long lastPageId = fileLength / mPageSize;
    // TODO(elega) can batch multiple pages together ot speed up the loading.
    for (PageId pageId : pageIds) {
      int lengthToLoad = (int)
          (pageId.getPageIndex() == lastPageId ? fileLength % mPageSize : mPageSize);
      long offset = pageId.getPageIndex() * mPageSize;
      ByteBuf buf = PooledByteBufAllocator.DEFAULT.directBuffer(lengthToLoad);
      try (PositionReader reader = ufs.get().openPositionRead(ufsPath, fileLength)) {
        int bytesRead = reader.read(offset, buf, lengthToLoad);
        if (lengthToLoad != bytesRead) {
          throw new RuntimeException(
              "Page load failed, expected: " + lengthToLoad + " actual " + bytesRead);
        }
        mCacheManager.put(pageId, buf.nioBuffer());
      } finally {
        buf.release();
      }
    }
  }

  private void loadPages(
      String ufsPath, long mountId, long offset, long lengthToLoad, long fileLength)
      throws AccessControlException, IOException {
    if (lengthToLoad == 0) {
      return;
    }
    lengthToLoad = Math.min(lengthToLoad, fileLength - offset);
    List<PageId> pagesToLoad = new ArrayList<>();
    String fileId = new AlluxioURI(ufsPath).hash();
    for (long current = offset; current < offset + lengthToLoad; current += mPageSize) {
      pagesToLoad.add(new PageId(fileId, current / mPageSize));
    }
    loadPages(ufsPath, pagesToLoad, fileLength);
  }

  protected void loadData(String ufsPath, long mountId, long offset, long lengthToLoad,
      long fileLength) throws AccessControlException, IOException {
    Protocol.OpenUfsBlockOptions options =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(ufsPath).setMountId(mountId)
                                    .setNoCache(false).setOffsetInFile(offset)
                                    .setBlockSize(fileLength).build();
    String fileId = new AlluxioURI(ufsPath).hash();
    int bufferSize = (int) Math.min(4 * mPageSize, lengthToLoad);
    ByteBuf buf =
        PooledByteBufAllocator.DEFAULT.directBuffer(bufferSize);
    try (BlockReader fileReader = createFileReader(fileId, offset, false, options)) {
      //Transfers data from this reader to the buffer until we reach lengthToLoad.
      int bytesRead;
      while (lengthToLoad > 0 && (bytesRead = fileReader.transferTo(buf)) != -1) {
        lengthToLoad -= bytesRead;
        buf.clear();
        if (lengthToLoad < bufferSize) {
          buf.capacity((int) Math.min(lengthToLoad, bufferSize));
        }
      }
    } catch (IOException | AccessControlException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      buf.release();
    }
  }

  /**
   * Loads data from remote worker.
   *
   * @param filePath     the file path
   * @param offset       the offset
   * @param lengthToLoad the length to load
   * @param reader       the netty reader
   * @param chunkSize    the chunk size
   * @throws IOException when failed to read from remote worker
   */
  @VisibleForTesting
  public void loadDataFromRemote(String filePath, long offset, long lengthToLoad,
      PositionReader reader, int chunkSize) throws IOException {
    ByteBuffer buf = ByteBuffer.allocateDirect(chunkSize);
    String fileId = new AlluxioURI(filePath).hash();
    while (lengthToLoad > 0) {
      long currentPageIndex = offset / mPageSize;
      PageId pageId = new PageId(fileId.toString(), currentPageIndex);
      int lengthToRead = (int) Math.min(chunkSize, lengthToLoad);
      int lengthRead = reader.read(offset, buf, lengthToRead);
      if (lengthRead != lengthToRead) {
        throw new FailedPreconditionRuntimeException(
            "Read " + lengthRead + " bytes, expected to read " + lengthToRead + " bytes");
      }
      buf.flip();
      mCacheManager.put(pageId, buf);
      offset += lengthRead;
      lengthToLoad -= lengthRead;
      buf.clear();
    }
  }

  @Override
  public ListenableFuture<List<RouteFailure>> copy(List<Route> routes, UfsReadOptions readOptions,
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
          try {
            if (readOptions.hasUser()) {
              AuthenticatedClientUser.set(readOptions.getUser());
            }
            checkCopyPermission(route.getSrc(), route.getDst());
            if (!ValidateHandler.validate(route, writeOptions, srcFs, dstFs, false)) {
              // Skip copy if there is a failure during validation.
              RouteFailure.Builder builder =
                  RouteFailure.newBuilder().setRoute(route).setIsSkip(true).setCode(0);
              errors.add(builder.build());
              return;
            }
            CopyHandler.copy(route, writeOptions, srcFs, dstFs);
          } catch (Throwable t) {
            boolean permissionCheckSucceeded = !(t instanceof AccessControlException);
            LOG.error("Failed to copy {} to {}", route.getSrc(), route.getDst(), t);
            AlluxioRuntimeException e = AlluxioRuntimeException.from(t);
            RouteFailure.Builder builder =
                RouteFailure.newBuilder().setRoute(route).setCode(e.getStatus().getCode().value())
                    .setRetryable(e.isRetryable() && permissionCheckSucceeded)
                    .setIsSkip(false);
            if (e.getMessage() != null) {
              builder.setMessage(e.getMessage());
            }
            errors.add(builder.build());
          }
        }, GrpcExecutors.WRITER_EXECUTOR);
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
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.WRITER_EXECUTOR);
  }

  protected UnderFileSystem getUnderFileSystem(String ufsPath) {
    return mUfsManager.getOrAdd(new AlluxioURI(ufsPath),
        () -> UnderFileSystemConfiguration.defaults(mConf));
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
            if (!ValidateHandler.validate(route, writeOptions, srcFs, dstFs, true)) {
              throw new FailedPreconditionRuntimeException("File " + route.getDst()
                  + " is already in UFS");
            }
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
        }, GrpcExecutors.WRITER_EXECUTOR);
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
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.WRITER_EXECUTOR);
  }

  @Override
  public OpenFileHandle createFile(String path, CreateFilePOptions options)
      throws AccessControlException, IOException {
    UnderFileSystem ufs = getUfsInstance(path);
    // TODO(yuyang): Lock is needed.
    alluxio.grpc.FileInfo info;
    OpenFileHandle existingHandle = mOpenFileHandleContainer.find(path);
    if (existingHandle != null) {
      LOG.error("A file opened for write and not closed yet: path={} handle={}",
          path, existingHandle);
      // If want to enable this checking and throw exception, we need to handle such abnormal cases:
      // 1. If client disconnects without sending CompleteFile request, we must have a way to
      //    clean up the stale handle. Please see DoraOpenFileHandleContainer::run().
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
      boolean exists = ufs.exists(path);
      if (!overWrite && exists) {
        throw new AlreadyExistsException(String.format("File %s already exists"
            + "but no overwrite flag", path));
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
      info = buildFileInfoFromUfsStatus(mCacheManager.getUsage(),
          getUfsInstance(path).getUnderFSType(), status, path, null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    OutputStream outStream;
    if (mClientWriteToUFSEnabled) {
      // client is writing directly to UFS. Worker does not write to UFS.
      outStream = null;
    } else {
      outStream = ufs.create(path, createOption);
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
    UnderFileSystem ufs = getUfsInstance(path);
    try {
      mMetaManager.removeFromMetaStore(path);

      // TODO(hua) Close the open file handle?
      if (!options.getAlluxioOnly()) {
        // By being a cache, Dora assume the file exists in UFS when a delete is issued
        // So if the file does not exist in UFS, an IOException will be thrown here
        UfsStatus status = ufs.getStatus(path);
        if (status.isFile()) {
          ufs.deleteFile(path);
        } else {
          if (options.hasRecursive() && options.getRecursive()) {
            ufs.deleteDirectory(path, DeleteOptions.RECURSIVE);
          } else {
            ufs.deleteDirectory(path, DeleteOptions.NON_RECURSIVE);
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
    UnderFileSystem srcUfs = getUfsInstance(src);
    UnderFileSystem dstUfs = getUfsInstance(dst);
    LOG.debug("Renaming from {} to {}", src, dst);
    // use strong reference comparison as UnderFileSystem does not support equality check
    // except by UFS type
    if (srcUfs != dstUfs) {
      throw new FailedPreconditionException("Cannot rename a file in one UFS to another UFS");
    }

    try {
      // Check if the target file already exists. If yes, return by throwing error.
      boolean overWrite = options.hasOverwrite() ? options.getOverwrite() : false;
      boolean exists = srcUfs.exists(dst);
      if (!overWrite && exists) {
        throw new AlreadyExistsRuntimeException(String.format("File %s already exists but"
            + "no overwrite flag", dst));
      } else if (overWrite) {
        mMetaManager.removeFromMetaStore(dst);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    boolean rc;
    try {
      UfsStatus status = srcUfs.getStatus(src);
      if (status.isFile()) {
        rc = srcUfs.renameFile(src, dst);
      } else {
        rc = srcUfs.renameDirectory(src, dst);
      }
      if (rc) {
        mMetaManager.removeFromMetaStore(src);
        mMetaManager.loadFromUfs(dst);
        mMetaManager.invalidateListingCacheOfParent(dst);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    LOG.debug("Renaming from {} to {} done: {}", src, dst, rc);
    if (!rc) {
      throw new RuntimeException(String.format("Failed to rename from '%s' to '%s'", src, dst));
    }
  }

  @Override
  public void createDirectory(String path, CreateDirectoryPOptions options)
      throws IOException, AccessControlException {
    UnderFileSystem ufs = getUfsInstance(path);
    try {
      boolean success;
      if (options.hasRecursive() && options.getRecursive()) {
        success = ufs.mkdirs(path, mMkdirsRecursive);
      } else {
        success = ufs.mkdirs(path, mMkdirsNonRecursive);
      }
      mMetaManager.loadFromUfs(path);
      mMetaManager.invalidateListingCacheOfParent(path);
      if (!success) {
        throw new AlreadyExistsException(String.format("%s already exists", path));
      }
    } catch (IOException e) {
      // IOE would be caught by AlluxioRuntimeException
      throw e;
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

    UnderFileSystem ufs = getUfsInstance(path);
    if (options.hasMode()) {
      ufs.setMode(path, ModeUtils.protoToShort(options.getMode()));
    }
    if (options.hasOwner() && options.hasGroup()) {
      ufs.setOwner(path, options.getOwner(), options.getGroup());
    } else if (options.hasOwner()) {
      ufs.setOwner(path, options.getOwner(), null);
    } else if (options.hasGroup()) {
      ufs.setOwner(path, null, options.getGroup());
    }
    if (options.getXattrCount() > 0) {
      if (mXAttrWriteToUFSEnabled) {
        Map<String, ByteString> xattr = options.getXattrMap();
        for (Map.Entry<String, ByteString> attr : xattr.entrySet()) {
          ufs.setAttribute(path, attr.getKey(), attr.getValue().toByteArray());
        }
      }
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
      WorkerIdentity identity = mWorkerId.get();
      Preconditions.checkState(identity != null, "worker not registered");
      final long numericId;
      try {
        numericId = WorkerIdentity.ParserV0.INSTANCE.toLong(identity);
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException("Worker is using identity based on UUID, which should "
            + "not be doing block master sync", e);
      }
      final Command cmdFromMaster;
      try (PooledResource<BlockMasterClient> bmc = mBlockMasterClientPool.acquireCloseable()) {
        cmdFromMaster = bmc.get().heartbeat(numericId,
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
  DoraMetaManager getMetaManager() {
    return mMetaManager;
  }

  /**
   * Gets the current cache usage in worker.
   * @return cache usage
   */
  public Optional<CacheUsage> getCacheUsage() {
    return mCacheManager.getUsage();
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

  @Override
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * Build FileInfo from UfsStatus and UFS full Path.
   *
   * @param cacheUsage cache usage
   * @param ufsType type of the UFS
   * @param status file status
   * @param ufsFullPath full UFS path mapping to the file
   * @param xattrMap extra attributes
   * @return a FileInfo
   */
  public static alluxio.grpc.FileInfo buildFileInfoFromUfsStatus(
      Optional<CacheUsage> cacheUsage, String ufsType, UfsStatus status, String ufsFullPath,
      @Nullable Map<String, String> xattrMap) {
    String filename = new AlluxioURI(ufsFullPath).getName();

    alluxio.grpc.FileInfo.Builder infoBuilder = alluxio.grpc.FileInfo.newBuilder()
        .setUfsType(ufsType)
        .setFileId(ufsFullPath.hashCode())
        .setName(filename)
        .setPath(ufsFullPath)
        .setUfsPath(ufsFullPath)
        .setMode(status.getMode())
        .setFolder(status.isDirectory())
        .setOwner(status.getOwner())
        .setGroup(status.getGroup())
        .setCompleted(true)
        .setPersisted(true);
    if (xattrMap != null) {
      for (Map.Entry<String, String> entry : xattrMap.entrySet()) {
        infoBuilder.putXattr(entry.getKey(), ByteString.copyFromUtf8(entry.getValue()));
      }
    }
    if (status.getLastModifiedTime() != null) {
      infoBuilder.setLastModificationTimeMs(status.getLastModifiedTime());
    }
    if (status instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) status;
      infoBuilder.setLength(fileStatus.getContentLength())
          .setBlockSizeBytes(fileStatus.getBlockSize());
      String contentHash = ((UfsFileStatus) status).getContentHash();
      if (contentHash != null) {
        infoBuilder.setContentHash(contentHash);
      }

      // get cached percentage
      String cacheManagerFileId = new AlluxioURI(ufsFullPath).hash();
      final long bytesInCache = cacheUsage
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
   * @param cacheUsage cache usage
   * @param ufsType type of the UFS
   * @param status the ufs status
   * @param ufsFullPath the full ufs path
   * @param xattrMap the map of file xAttrs
   * @return the file status
   */
  public static DoraMeta.FileStatus buildFileStatusFromUfsStatus(
      Optional<CacheUsage> cacheUsage, String ufsType, UfsStatus status, String ufsFullPath,
      @Nullable Map<String, String> xattrMap) {
    return DoraMeta.FileStatus.newBuilder()
        .setFileInfo(buildFileInfoFromUfsStatus(cacheUsage, ufsType, status, ufsFullPath, xattrMap))
        .setTs(System.nanoTime())
        .build();
  }
}
