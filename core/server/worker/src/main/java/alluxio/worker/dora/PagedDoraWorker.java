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
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheUsage;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.NotFoundException;
import alluxio.file.FileId;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.File;
import alluxio.grpc.FileFailure;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.Scope;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.network.protocol.databuffer.PooledDirectNioByteBuf;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.meta.DoraMeta;
import alluxio.resource.PooledResource;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsInputStreamCache;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.grpc.GrpcExecutors;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Page store based dora worker.
 */
public class PagedDoraWorker extends AbstractWorker implements DoraWorker {
  private static final Logger LOG = LoggerFactory.getLogger(PagedDoraWorker.class);
  // for now Dora Worker does not support Alluxio <-> UFS mapping,
  // and assumes all UFS paths belong to the same UFS.
  private static final int MOUNT_POINT = 1;
  private final Closer mResourceCloser = Closer.create();
  private final AtomicReference<Long> mWorkerId;
  private final CacheManager mCacheManager;
  private final DoraUfsManager mUfsManager;
  private final UfsInputStreamCache mUfsStreamCache;
  private final long mPageSize;
  private final AlluxioConfiguration mConf;
  private final BlockMasterClientPool mBlockMasterClientPool;
  private final String mRootUFS;
  private final LoadingCache<String, DoraMeta.FileStatus> mUfsStatusCache;
  private final Cache<String, UfsStatus[]> mListStatusCache;
  private WorkerNetAddress mAddress;

  private RocksDBDoraMetaStore mMetaStore;
  private final UnderFileSystem mUfs;

  /**
   * Constructor.
   * @param workerId
   * @param conf
   * @param cacheManager
   */
  @Inject
  public PagedDoraWorker(
      AtomicReference<Long> workerId,
      AlluxioConfiguration conf,
      CacheManager cacheManager) {
    super(ExecutorServiceFactories.fixedThreadPool("dora-worker-executor", 5));
    mWorkerId = workerId;
    mConf = conf;
    mRootUFS = Configuration.getString(PropertyKey.DORA_CLIENT_UFS_ROOT);
    mUfsManager = mResourceCloser.register(new DoraUfsManager());
    mUfsStreamCache = new UfsInputStreamCache();
    mUfs = UnderFileSystem.Factory.create(
        mRootUFS,
        UnderFileSystemConfiguration.defaults(Configuration.global()));
    mUfsStatusCache = CacheBuilder.newBuilder()
        .maximumSize(Configuration.getInt(PropertyKey.DORA_WORKER_FILE_STATUS_MEM_CACHE_SIZE))
        .expireAfterWrite(Configuration.getDuration(
            PropertyKey.DORA_WORKER_FILE_STATUS_MEM_CACHE_TTL))
        .build(new CacheLoader<String, DoraMeta.FileStatus>() {
          @Override
          public DoraMeta.FileStatus load(String path) throws IOException {
            UfsStatus status = mUfs.getStatus(path);
            DoraMeta.FileStatus fs = buildFileStatusFromUfsStatus(status, path);
            return fs;
          }
        });
    mListStatusCache = CacheBuilder.newBuilder()
        .maximumSize(Configuration.getInt(PropertyKey.DORA_WORKER_LIST_STATUS_MEM_CACHE_SIZE))
        .expireAfterWrite(Configuration.getDuration(
            PropertyKey.DORA_WORKER_LIST_STATUS_MEM_CACHE_TTL))
        .build();

    mPageSize = Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    mBlockMasterClientPool = new BlockMasterClientPool();

    String dbDir = Configuration.getString(PropertyKey.DORA_WORKER_METADATA_ROCKSDB_CACHE_DIR);
    Duration duration = Configuration.getDuration(
        PropertyKey.DORA_WORKER_METADATA_ROCKSDB_CACHE_TTL);
    long ttl = duration.isNegative() ? -1 : duration.getSeconds();
    try {
      if (ttl == 0) {
        LOG.info("Worker MetaStore RocksDB TTL is configured to be ZERO. That means no cache!");
        mMetaStore = null;
      } else {
        mMetaStore = new RocksDBDoraMetaStore(dbDir, ttl);
      }
    } catch (RuntimeException e) {
      LOG.error("Cannot init RocksDBDoraMetaStore. Continue without MetaStore", e);
      mMetaStore = null;
    }
    mCacheManager = cacheManager;
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

    // setup worker-master heartbeat
    // the heartbeat is only used to notify the aliveness of this worker, so that clients
    // can get the latest worker list from master.
    // TODO(bowen): once we set up a worker discovery service in place of master, remove this
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC,
            mResourceCloser.register(new BlockMasterSync()),
            (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
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
    super.stop();
  }

  @Override
  public void close() throws IOException {
    if (mMetaStore != null) {
      mMetaStore.close();
    }
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
  public UfsStatus[] listStatus(String path, ListOptions options) throws IOException {
    UfsStatus[] statuses = mListStatusCache.getIfPresent(path);
    if (statuses == null) {
      // Not found in cache. Query the Under File System.
      statuses = mUfs.listStatus(path, options);

      if (statuses == null) {
        // If empty, the request path might be a regular file/object. Let's retry getStatus().
        try {
          UfsStatus status = mUfs.getStatus(path);
          // listStatus() expects relative name to the @path.
          status.setName("");
          statuses = new UfsStatus[1];
          statuses[0] = status;
        } catch (FileNotFoundException e) {
          statuses = null;
        }
      }

      // Add this into cache. Return value might be null if not found.
      if (statuses != null) {
        mListStatusCache.put(path, statuses);
      }
    }
    return statuses;
  }

  /**
   * @param fileInfo the FileInfo of this file. Cached pages are identified by PageId
   * @return true at this moment
   */
  @Override
  public boolean invalidateCachedFile(FileInfo fileInfo) {
    long pages = (fileInfo.getLength() + mPageSize - 1) / mPageSize;
    FileId file = FileId.of(new AlluxioURI(fileInfo.getUfsPath()).hash());

    // @TODO(huanghua78) Only invalidate cached pages. Here, we don't check if a page is
    // cached or not. If a page is not cached at all, the mCacheManager.delete() will log
    // error messages to complain the non-existance of the page. As an optimization we need
    // to only invalidate cached pages.
    for (long i = 0; i < pages; i++) {
      PageId page = new PageId(file.toString(), i);
      mCacheManager.delete(page);
    }
    return true;
  }

  @Override
  public FileInfo getFileInfo(String ufsFullPath, GetStatusPOptions options) throws IOException {
    alluxio.grpc.FileInfo fi;
    boolean invalidated = false;
    long syncIntervalMs = options.hasCommonOptions()
        ? (options.getCommonOptions().hasSyncIntervalMs()
          ? options.getCommonOptions().getSyncIntervalMs() : -1) :
        -1;

    DoraMeta.FileStatus status = mUfsStatusCache.getIfPresent(ufsFullPath);
    if (syncIntervalMs >= 0 && status != null) {
      // Check if the metadata is still valid.
      if (System.nanoTime() - status.getTs() > syncIntervalMs * Constants.MS_NANO) {
        // The metadata is expired. Remove it from in-memory cache.
        mUfsStatusCache.invalidate(ufsFullPath);
        invalidated = invalidateCachedFile(GrpcUtils.fromProto(status.getFileInfo()));
        status = null;
      }
    }
    if (status == null) {
      // The requested FileStatus is not present in memory cache.
      // Let's try to query local persistent DoraMetaStore.
      Optional<DoraMeta.FileStatus> fs;
      if (mMetaStore != null) {
        fs = mMetaStore.getDoraMeta(ufsFullPath);
      } else {
        // The MetaStore is not ready. Treat this as not found.
        fs = Optional.empty();
      }
      if (syncIntervalMs >= 0 && fs.isPresent()) {
        // Check if the metadata is still valid.
        if (System.nanoTime() - fs.get().getTs() > syncIntervalMs * Constants.MS_NANO) {
          // The metadata is expired. Remove it from RocksDB.
          if (mMetaStore != null) {
            mMetaStore.removeDoraMeta(ufsFullPath);
            if (!invalidated) {
              invalidated = invalidateCachedFile(GrpcUtils.fromProto(fs.get().getFileInfo()));
            }
          }
          fs = Optional.empty();
        }
      }

      if (fs.isPresent()) {
        // Found in persistent DoraMetaStore
        fi = fs.get().getFileInfo();
        mUfsStatusCache.put(ufsFullPath, fs.get());
      } else {
        // This will load UfsFileStatus from UFS and put it in memory cache
        try {
          status = mUfsStatusCache.get(ufsFullPath);
        } catch (ExecutionException e) {
          Throwable throwable = e.getCause();
          // this should be the exception thrown by ufs.getFileStatus which is IOException
          if (throwable instanceof IOException) {
            throw (IOException) throwable;
          } else {
            throw new InternalException("Unexpected exception when retrieving UFS file status",
                throwable);
          }
        }
        if (mMetaStore != null) {
          mMetaStore.putDoraMeta(ufsFullPath, status);
          if (!invalidated) {
            invalidateCachedFile(GrpcUtils.fromProto(status.getFileInfo()));
          }
        }
        fi = status.getFileInfo();
      }
    } else {
      fi = status.getFileInfo();
    }
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
      cachedPercentage = 0;
    }
    return GrpcUtils.fromProto(fi)
        .setInAlluxioPercentage(cachedPercentage)
        .setInMemoryPercentage(cachedPercentage);
  }

  /**
   * Build FileInfo from UfsStatus and UFS full Path.
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
        .setFileId(ufsFullPath.hashCode())
        .setName(filename)
        .setPath(relativePath)
        .setUfsPath(ufsFullPath)
        .setMode(status.getMode())
        .setFolder(status.isDirectory())
        .setOwner(status.getOwner())
        .setGroup(status.getGroup())
        .setCompleted(true);
    if (status instanceof UfsFileStatus) {
      UfsFileStatus fileStatus = (UfsFileStatus) status;
      infoBuilder.setLength(fileStatus.getContentLength())
          .setLastModificationTimeMs(status.getLastModifiedTime())
          .setBlockSizeBytes(fileStatus.getBlockSize());
    }
    return infoBuilder.build();
  }

  /**
   * Build FileStatus from UfsStatus and UFS full Path.
   * @param status
   * @param ufsFullPath
   * @return
   */
  private  DoraMeta.FileStatus buildFileStatusFromUfsStatus(UfsStatus status, String ufsFullPath) {
    return DoraMeta.FileStatus.newBuilder()
        .setFileInfo(buildFileInfoFromUfsStatus(status, ufsFullPath))
        .setTs(System.nanoTime())
        .build();
  }

  @Override
  public BlockReader createFileReader(String fileId, long offset, boolean positionShort,
      Protocol.OpenUfsBlockOptions options) throws IOException {
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
  public ListenableFuture<List<FileFailure>> load(List<File> files) {
    List<ListenableFuture<Void>> futures = new ArrayList<>();
    List<FileFailure> errors = Collections.synchronizedList(new ArrayList<>());
    for (File file : files) {
      ListenableFuture<Void> loadFuture = Futures.submit(() -> {
        try {
          load(file.getUfsPath(), file.getMountId(), file.getLength());
        } catch (Exception e) {
          AlluxioRuntimeException t = AlluxioRuntimeException.from(e);
          errors.add(FileFailure.newBuilder().setFile(file).setCode(t.getStatus().getCode().value())
                                .setMessage(t.getMessage()).build());
        }
      }, GrpcExecutors.BLOCK_READER_EXECUTOR);
      futures.add(loadFuture);
    }
    return Futures.whenAllComplete(futures).call(() -> errors, GrpcExecutors.BLOCK_READER_EXECUTOR);
  }

  private void load(String ufsPath, long mountId, long length) {
    Protocol.OpenUfsBlockOptions options =
        Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(ufsPath).setMountId(mountId)
                                    .setNoCache(false).setOffsetInFile(0).setBlockSize(length)
                                    .build();
    String fileId = new AlluxioURI(ufsPath).hash();
    ByteBuf buf = PooledDirectNioByteBuf.allocate((int) (4 * mPageSize));
    try (BlockReader fileReader = createFileReader(fileId, 0, false, options)) {
      while (fileReader.transferTo(buf) != -1) {
        buf.clear();
      }
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
    finally {
      buf.release();
    }
  }

  @Override
  public void cleanupSession(long sessionId) {
  }

  private class BlockMasterSync implements HeartbeatExecutor {
    @Override
    public void heartbeat() throws InterruptedException {
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
}
