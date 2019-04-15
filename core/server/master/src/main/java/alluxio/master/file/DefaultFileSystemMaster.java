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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.Server;
import alluxio.client.job.JobMasterClient;
import alluxio.client.job.JobMasterClientPool;
import alluxio.clock.SystemClock;
import alluxio.collections.Pair;
import alluxio.collections.PrefixList;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.ServiceType;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.ProtobufUtils;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.master.audit.AuditContext;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.DefaultBlockMaster;
import alluxio.master.file.activesync.ActiveSyncManager;
import alluxio.master.file.contexts.CheckConsistencyContext;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.LoadMetadataContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.contexts.WorkerHeartbeatContext;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodePathPair;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsBlockLocationCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.JournalEntryIterable;
import alluxio.master.journal.JournalUtils;
import alluxio.master.journal.Journaled;
import alluxio.master.journal.checkpoint.CheckpointInputStream;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.DelegatingReadOnlyInodeStore;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.master.metrics.TimeSeriesStore;
import alluxio.metrics.MasterMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.TimeSeries;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddSyncPointEntry;
import alluxio.proto.journal.File.NewBlockEntry;
import alluxio.proto.journal.File.RemoveSyncPointEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAclEntry;
import alluxio.proto.journal.File.UpdateInodeEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry;
import alluxio.proto.journal.File.UpdateInodeFileEntry.Builder;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableResource;
import alluxio.resource.LockResource;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authentication.ClientIpAddressInjector;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.AclEntryType;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.Fingerprint.Tag;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ModeUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.StreamUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.CommandType;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.FileSystemCommandOptions;
import alluxio.wire.MountPointInfo;
import alluxio.wire.PersistCommandOptions;
import alluxio.wire.PersistFile;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.UfsInfo;
import alluxio.wire.WorkerInfo;
import alluxio.worker.job.JobMasterClientContext;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.grpc.ServerInterceptors;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The master that handles all file system metadata management.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class DefaultFileSystemMaster extends CoreMaster implements FileSystemMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSystemMaster.class);
  private static final Set<Class<? extends Server>> DEPS = ImmutableSet.of(BlockMaster.class);

  /** The number of threads to use in the {@link #mPersistCheckerPool}. */
  private static final int PERSIST_CHECKER_POOL_THREADS = 128;

  /**
   * Locking in DefaultFileSystemMaster
   *
   * Individual paths are locked in the inode tree. In order to read or write any inode, the path
   * must be locked. A path is locked via one of the lock methods in {@link InodeTree}, such as
   * {@link InodeTree#lockInodePath(AlluxioURI, LockMode)} or
   * {@link InodeTree#lockFullInodePath(AlluxioURI, LockMode)}. These lock methods return
   * an {@link LockedInodePath}, which represents a locked path of inodes. These locked paths
   * ({@link LockedInodePath}) must be unlocked. In order to ensure a locked
   * {@link LockedInodePath} is always unlocked, the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, LockPattern.READ)) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * When locking a path in the inode tree, it is possible that other concurrent operations have
   * modified the inode tree while a thread is waiting to acquire a lock on the inode. Lock
   * acquisitions throw {@link InvalidPathException} to indicate that the inode structure is no
   * longer consistent with what the caller original expected, for example if the inode
   * previously obtained at /pathA has been renamed to /pathB during the wait for the inode lock.
   * Methods which specifically act on a path will propagate this exception to the caller, while
   * methods which iterate over child nodes can safely ignore the exception and treat the inode
   * as no longer a child.
   *
   * JournalContext, BlockDeletionContext, and RpcContext
   *
   * RpcContext is an aggregator for various contexts which get passed around through file system
   * master methods.
   *
   * Currently there are two types of contexts that get passed around: {@link JournalContext} and
   * {@link BlockDeletionContext}. These contexts are used to register work that should be done when
   * the context closes. The journal context tracks journal entries which need to be flushed, while
   * the block deletion context tracks which blocks need to be deleted in the {@link BlockMaster}.
   *
   * File system master journal entries should be written before blocks are deleted in the block
   * master, so journal context should always be closed before block deletion context. In order to
   * ensure that contexts are closed and closed in the right order, the following paradign is
   * recommended:
   *
   * <p><blockquote><pre>
   *    try (RpcContext rpcContext = createRpcContext()) {
   *      // access journal context with rpcContext.getJournalContext()
   *      // access block deletion context with rpcContext.getBlockDeletionContext()
   *      ...
   *    }
   * </pre></blockquote>
   *
   * When used in conjunction with {@link LockedInodePath} and {@link AuditContext}, the usage
   * should look like
   *
   * <p><blockquote><pre>
   *    try (RpcContext rpcContext = createRpcContext();
   *         LockedInodePath inodePath = mInodeTree.lockInodePath(...);
   *         FileSystemMasterAuditContext auditContext = createAuditContext(...)) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * NOTE: Because resources are released in the opposite order they are acquired, the
   * {@link JournalContext}, {@link BlockDeletionContext}, or {@link RpcContext} resources should be
   * always created before any {@link LockedInodePath} resources to avoid holding an inode path lock
   * while waiting for journal IO.
   *
   * User access audit logging in the FileSystemMaster
   *
   * User accesses to file system metadata should be audited. The intent to write audit log and the
   * actual writing of the audit log is decoupled so that operations are not holding metadata locks
   * waiting on the audit log IO. In particular {@link AsyncUserAccessAuditLogWriter} uses a
   * separate thread to perform actual audit log IO. In order for audit log entries to preserve
   * the order of file system operations, the intention of auditing should be submitted to
   * {@link AsyncUserAccessAuditLogWriter} while holding locks on the inode path. That said, the
   * {@link AuditContext} resources should always live within the scope of {@link LockedInodePath},
   * i.e. created after {@link LockedInodePath}. Otherwise, the order of audit log entries may not
   * reflect the actual order of the user accesses.
   * Resources are released in the opposite order they are acquired, the
   * {@link AuditContext#close()} method is called before {@link LockedInodePath#close()}, thus
   * guaranteeing the order.
   *
   * Method Conventions in the FileSystemMaster
   *
   * All of the flow of the FileSystemMaster follow a convention. There are essentially 4 main
   * types of methods:
   *   (A) public api methods
   *   (B) private (or package private) internal methods
   *
   * (A) public api methods:
   * These methods are public and are accessed by the RPC and REST APIs. These methods lock all
   * the required paths, and also perform all permission checking.
   * (A) cannot call (A)
   * (A) can call (B)
   *
   * (B) private (or package private) internal methods:
   * These methods perform the rest of the work. The names of these
   * methods are suffixed by "Internal". These are typically called by the (A) methods.
   * (B) cannot call (A)
   * (B) can call (B)
   */

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;

  /** Store for holding inodes. */
  private final ReadOnlyInodeStore mInodeStore;

  /** This manages inode locking. */
  private final InodeLockManager mInodeLockManager;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /** This checks user permissions on different operations. */
  private final PermissionChecker mPermissionChecker;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /** A pool of job master clients. */
  private final JobMasterClientPool mJobMasterClientPool;

  /** Set of file IDs to persist. */
  private final Map<Long, alluxio.time.ExponentialTimer> mPersistRequests;

  /** Map from file IDs to persist jobs. */
  private final Map<Long, PersistJob> mPersistJobs;

  /** The manager of all ufs. */
  private final MasterUfsManager mUfsManager;

  /** This caches absent paths in the UFS. */
  private final UfsAbsentPathCache mUfsAbsentPathCache;

  /** This caches block locations in the UFS. */
  private final UfsBlockLocationCache mUfsBlockLocationCache;

  /** This caches paths which have been synced with UFS. */
  private final UfsSyncPathCache mUfsSyncPathCache;

  /** List of all master subcomponents which require journaling. */
  private final List<Journaled> mJournaledComponents;

  /** Thread pool which asynchronously handles the completion of persist jobs. */
  private java.util.concurrent.ThreadPoolExecutor mPersistCheckerPool;

  private Future<List<AlluxioURI>> mStartupConsistencyCheck;

  private ActiveSyncManager mSyncManager;

  /** Log writer for user access audit log. */
  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  /** Stores the time series for various metrics which are exposed in the UI. */
  private TimeSeriesStore mTimeSeriesStore;

  /**
   * Creates a new instance of {@link DefaultFileSystemMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   */
  public DefaultFileSystemMaster(BlockMaster blockMaster, CoreMasterContext masterContext) {
    this(blockMaster, masterContext,
        ExecutorServiceFactories.cachedThreadPool(Constants.FILE_SYSTEM_MASTER_NAME));
  }

  /**
   * Creates a new instance of {@link DefaultFileSystemMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  public DefaultFileSystemMaster(BlockMaster blockMaster, CoreMasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, new SystemClock(), executorServiceFactory);

    mBlockMaster = blockMaster;
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mUfsManager = new MasterUfsManager();
    mMountTable = new MountTable(mUfsManager, getRootMountInfo(mUfsManager));
    mInodeLockManager = new InodeLockManager();
    InodeStore inodeStore = masterContext.getInodeStoreFactory().apply(mInodeLockManager);
    mInodeStore = new DelegatingReadOnlyInodeStore(inodeStore);
    mInodeTree = new InodeTree(inodeStore, mBlockMaster,
        mDirectoryIdGenerator, mMountTable, mInodeLockManager);

    // TODO(gene): Handle default config value for whitelist.
    mWhitelist = new PrefixList(ServerConfiguration.getList(PropertyKey.MASTER_WHITELIST, ","));

    mPermissionChecker = new DefaultPermissionChecker(mInodeTree);
    mJobMasterClientPool = new JobMasterClientPool(JobMasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    mPersistRequests = new java.util.concurrent.ConcurrentHashMap<>();
    mPersistJobs = new java.util.concurrent.ConcurrentHashMap<>();
    mUfsAbsentPathCache = UfsAbsentPathCache.Factory.create(mMountTable);
    mUfsBlockLocationCache = UfsBlockLocationCache.Factory.create(mMountTable);
    mUfsSyncPathCache = new UfsSyncPathCache();
    mSyncManager = new ActiveSyncManager(mMountTable, this);
    mTimeSeriesStore = new TimeSeriesStore();
    // The mount table should come after the inode tree because restoring the mount table requires
    // that the inode tree is already restored.
    mJournaledComponents = new ArrayList<Journaled>() {
      {
        add(mInodeTree);
        add(mDirectoryIdGenerator);
        add(mMountTable);
        add(mUfsManager);
        add(mSyncManager);
      }
    };

    resetState();
    Metrics.registerGauges(this, mUfsManager);
  }

  private static MountInfo getRootMountInfo(MasterUfsManager ufsManager) {
    try (CloseableResource<UnderFileSystem> resource = ufsManager.getRoot().acquireUfsResource()) {
      String rootUfsUri = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
      boolean shared = resource.get().isObjectStorage()
          && ServerConfiguration.getBoolean(PropertyKey.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY);
      Map<String, String> rootUfsConf =
          ServerConfiguration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
      MountPOptions mountOptions = MountContext
          .mergeFrom(MountPOptions.newBuilder().setShared(shared).putAllProperties(rootUfsConf))
          .getOptions().build();
      return new MountInfo(new AlluxioURI(MountTable.ROOT),
          new AlluxioURI(rootUfsUri), IdUtils.ROOT_MOUNT_ID, mountOptions);
    }
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.FILE_SYSTEM_MASTER_CLIENT_SERVICE, new GrpcService(ServerInterceptors
        .intercept(new FileSystemMasterClientServiceHandler(this), new ClientIpAddressInjector())));
    services.put(ServiceType.FILE_SYSTEM_MASTER_JOB_SERVICE,
        new GrpcService(new FileSystemMasterJobServiceHandler(this)));
    services.put(ServiceType.FILE_SYSTEM_MASTER_WORKER_SERVICE,
        new GrpcService(new FileSystemMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    for (Journaled journaled : mJournaledComponents) {
      if (journaled.processJournalEntry(entry)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void resetState() {
    mJournaledComponents.forEach(Journaled::resetState);
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.FILE_SYSTEM_MASTER;
  }

  @Override
  public void writeToCheckpoint(OutputStream output) throws IOException, InterruptedException {
    JournalUtils.writeToCheckpoint(output, mJournaledComponents);
  }

  @Override
  public void restoreFromCheckpoint(CheckpointInputStream input) throws IOException {
    JournalUtils.restoreFromCheckpoint(input, mJournaledComponents);
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    List<Iterator<JournalEntry>> componentIters = StreamUtils
        .map(JournalEntryIterable::getJournalEntryIterator, mJournaledComponents);
    return Iterators.concat(componentIters.iterator());
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    if (isPrimary) {
      LOG.info("Starting fs master as primary");

      InodeDirectory root = mInodeTree.getRoot();
      if (root == null) {
        try (JournalContext context = createJournalContext()) {
          mInodeTree.initializeRoot(
              SecurityUtils.getOwnerFromLoginModule(ServerConfiguration.global()),
              SecurityUtils.getGroupFromLoginModule(ServerConfiguration.global()),
              ModeUtils.applyDirectoryUMask(Mode.createFullAccess(),
                  ServerConfiguration.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK)),
              context);
        }
      } else {
        // For backwards-compatibility:
        // Empty root owner indicates that previously the master had no security. In this case, the
        // master is allowed to be started with security turned on.
        String serverOwner = SecurityUtils.getOwnerFromLoginModule(ServerConfiguration.global());
        if (SecurityUtils.isSecurityEnabled(ServerConfiguration.global())
            && !root.getOwner().isEmpty() && !root.getOwner().equals(serverOwner)) {
          // user is not the previous owner
          throw new PermissionDeniedException(ExceptionMessage.PERMISSION_DENIED.getMessage(String
              .format("Unauthorized user on root. inode owner: %s current user: %s",
                  root.getOwner(), serverOwner)));
        }
      }

      // Initialize the ufs manager from the mount table.
      for (String key : mMountTable.getMountTable().keySet()) {
        if (key.equals(MountTable.ROOT)) {
          continue;
        }
        MountInfo mountInfo = mMountTable.getMountTable().get(key);
        UnderFileSystemConfiguration ufsConf =
            UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
            .createMountSpecificConf(mountInfo.getOptions().getPropertiesMap());
        mUfsManager.addMount(mountInfo.getMountId(), new AlluxioURI(key), ufsConf);
      }
      // Startup Checks and Periodic Threads.

      // Rebuild the list of persist jobs (mPersistJobs) and map of pending persist requests
      // (mPersistRequests)
      for (Long id : mInodeTree.getToBePersistedIds()) {
        Inode inode = mInodeStore.get(id).get();
        if (inode.isDirectory()) {
          continue;
        }
        if (inode.getPersistenceState() != PersistenceState.TO_BE_PERSISTED) {
          continue;
        }
        InodeFile inodeFile = inode.asFile();
        if (inodeFile.getPersistJobId() == Constants.PERSISTENCE_INVALID_JOB_ID) {
          mPersistRequests.put(inodeFile.getId(), new alluxio.time.ExponentialTimer(
              ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS),
              ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_INTERVAL_MS),
              ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS),
              ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS)));
        } else {
          AlluxioURI path;
          try {
            path = mInodeTree.getPath(inodeFile);
          } catch (FileDoesNotExistException e) {
            LOG.error("Failed to determine path for inode with id {}", id, e);
            continue;
          }
          addPersistJob(id, inodeFile.getPersistJobId(), path, inodeFile.getTempUfsPath());
        }
      }
      if (ServerConfiguration
          .getBoolean(PropertyKey.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED)) {
        validateInodeBlocks(true);
      }

      int blockIntegrityCheckInterval = (int) ServerConfiguration
          .getMs(PropertyKey.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL);

      if (blockIntegrityCheckInterval > 0) { // negative or zero interval implies disabled
        getExecutorService().submit(
            new HeartbeatThread(HeartbeatContext.MASTER_BLOCK_INTEGRITY_CHECK,
                new BlockIntegrityChecker(this), blockIntegrityCheckInterval,
                ServerConfiguration.global()));
      }
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK,
              new InodeTtlChecker(this, mInodeTree),
              (int) ServerConfiguration.getMs(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS),
              ServerConfiguration.global()));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
              new LostFileDetector(this, mInodeTree),
              (int) ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL),
              ServerConfiguration.global()));
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_REPLICATION_CHECK,
          new alluxio.master.file.replication.ReplicationChecker(mInodeTree, mBlockMaster,
              mSafeModeManager, mJobMasterClientPool),
          (int) ServerConfiguration.getMs(PropertyKey.MASTER_REPLICATION_CHECK_INTERVAL_MS),
          ServerConfiguration.global()));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_PERSISTENCE_SCHEDULER,
              new PersistenceScheduler(),
              (int) ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS),
              ServerConfiguration.global()));
      mPersistCheckerPool =
          new java.util.concurrent.ThreadPoolExecutor(PERSIST_CHECKER_POOL_THREADS,
              PERSIST_CHECKER_POOL_THREADS, 1, java.util.concurrent.TimeUnit.MINUTES,
              new LinkedBlockingQueue<Runnable>(),
              alluxio.util.ThreadFactoryUtils.build("Persist-Checker-%d", true));
      mPersistCheckerPool.allowCoreThreadTimeOut(true);
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_PERSISTENCE_CHECKER,
              new PersistenceChecker(),
              (int) ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS),
              ServerConfiguration.global()));
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_METRICS_TIME_SERIES,
              new TimeSeriesRecorder(),
              (int) ServerConfiguration.getMs(PropertyKey.MASTER_METRICS_TIME_SERIES_INTERVAL),
              ServerConfiguration.global()));
      if (ServerConfiguration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
        mStartupConsistencyCheck = getExecutorService().submit(() -> startupCheckConsistency(
            ExecutorServiceFactories
               .fixedThreadPool("startup-consistency-check", 32).create()));
      }
      if (ServerConfiguration.getBoolean(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED)) {
        mAsyncAuditLogWriter = new AsyncUserAccessAuditLogWriter();
        mAsyncAuditLogWriter.start();
      }
      if (ServerConfiguration.getBoolean(PropertyKey.UNDERFS_CLEANUP_ENABLED)) {
        getExecutorService().submit(
            new HeartbeatThread(HeartbeatContext.MASTER_UFS_CLEANUP, new UfsCleaner(this),
                (int) ServerConfiguration.getMs(PropertyKey.UNDERFS_CLEANUP_INTERVAL),
                ServerConfiguration.global()));
      }
      mSyncManager.start();
    }
  }

  @Override
  public void stop() throws IOException {
    if (mAsyncAuditLogWriter != null) {
      mAsyncAuditLogWriter.stop();
      mAsyncAuditLogWriter = null;
    }
    mSyncManager.stop();
    super.stop();
  }

  @Override
  public void validateInodeBlocks(boolean repair) throws UnavailableException {
    mBlockMaster.validateBlocks((blockId) -> {
      long fileId = IdUtils.fileIdFromBlockId(blockId);
      return mInodeTree.inodeIdExists(fileId);
    }, repair);
  }

  /**
   * Checks the consistency of the root in a multi-threaded and incremental fashion. This method
   * will only READ lock the directories and files actively being checked and release them after the
   * check on the file / directory is complete.
   *
   * @return a list of paths in Alluxio which are not consistent with the under storage
   */
  private List<AlluxioURI> startupCheckConsistency(final ExecutorService service)
      throws InterruptedException {
    /** A marker {@link StartupConsistencyChecker}s add to the queue to signal completion */
    final long completionMarker = -1;
    /** A shared queue of directories which have yet to be checked */
    final BlockingQueue<Long> dirsToCheck = new LinkedBlockingQueue<>();

    /**
     * A {@link Callable} which checks the consistency of a directory.
     */
    final class StartupConsistencyChecker implements Callable<List<AlluxioURI>> {
      /** The path to check, guaranteed to be a directory in Alluxio. */
      private final Long mFileId;

      /**
       * Creates a new callable which checks the consistency of a directory.
       * @param fileId the path to check
       */
      private StartupConsistencyChecker(Long fileId) {
        mFileId = fileId;
      }

      /**
       * Checks the consistency of the directory and all immediate children which are files. All
       * immediate children which are directories are added to the shared queue of directories to
       * check. The parent directory is READ locked during the entire call while the children are
       * READ locked only during the consistency check of the children files.
       *
       * @return a list of inconsistent uris
       */
      @Override
      public List<AlluxioURI> call() throws IOException {
        List<AlluxioURI> inconsistentUris = new ArrayList<>();
        try (LockedInodePath dir = mInodeTree.lockFullInodePath(mFileId, LockPattern.READ)) {
          if (!checkConsistencyInternal(dir)) {
            inconsistentUris.add(dir.getUri());
          }
          for (Inode child : mInodeStore.getChildren(dir.getInode().asDirectory())) {
            try (LockedInodePath childPath = dir.lockChild(child, LockPattern.READ)) {
              if (child.isDirectory()) {
                dirsToCheck.add(child.getId());
              } else {
                if (!checkConsistencyInternal(childPath)) {
                  inconsistentUris.add(childPath.getUri());
                }
              }
            } catch (InvalidPathException e) {
              // Inode is no longer a child, continue.
              continue;
            }
          }
        } catch (FileDoesNotExistException e) {
          // This should be safe, continue.
          LOG.debug("A file scheduled for consistency check was deleted before the check.");
        } catch (InvalidPathException e) {
          // This should not happen.
          LOG.error("An invalid path was discovered during the consistency check, skipping.", e);
        } catch (Throwable t) {
          LOG.error("Failed to check consistency", t);
          throw t;
        }
        dirsToCheck.add(completionMarker);
        return inconsistentUris;
      }
    }

    // Add the root to the directories to check.
    dirsToCheck.add(mInodeTree.getRoot().getId());
    List<Future<List<AlluxioURI>>> results = new ArrayList<>();
    // Tracks how many checkers have been started.
    long started = 0;
    // Tracks how many checkers have completed.
    long completed = 0;
    do {
      Long fileId = dirsToCheck.take();
      if (fileId == completionMarker) { // A thread signaled completion.
        completed++;
      } else { // A new directory needs to be checked.
        StartupConsistencyChecker checker = new StartupConsistencyChecker(fileId);
        results.add(service.submit(checker));
        started++;
      }
    } while (started != completed);

    // Return the total set of inconsistent paths discovered.
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    for (Future<List<AlluxioURI>> result : results) {
      try {
        inconsistentUris.addAll(result.get());
      } catch (Exception e) {
        // This shouldn't happen, all futures should be complete.
        throw new RuntimeException(e);
      }
    }
    service.shutdown();
    return inconsistentUris;
  }

  @Override
  public void cleanupUfs() {
    for (Map.Entry<String, MountInfo> mountPoint : mMountTable.getMountTable().entrySet()) {
      MountInfo info = mountPoint.getValue();
      if (info.getOptions().getReadOnly()) {
        continue;
      }
      try (CloseableResource<UnderFileSystem> ufsResource =
          mUfsManager.get(info.getMountId()).acquireUfsResource()) {
        ufsResource.get().cleanup();
      } catch (UnavailableException | NotFoundException e) {
        LOG.error("No UFS cached for {}", info, e);
      } catch (IOException e) {
        LOG.error("Failed in cleanup UFS {}.", info, e);
      }
    }
  }

  @Override
  public StartupConsistencyCheck getStartupConsistencyCheck() {
    if (!ServerConfiguration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
      return StartupConsistencyCheck.disabled();
    }
    if (mStartupConsistencyCheck == null) {
      return StartupConsistencyCheck.notStarted();
    }
    if (!mStartupConsistencyCheck.isDone()) {
      return StartupConsistencyCheck.running();
    }
    try {
      List<AlluxioURI> inconsistentUris = mStartupConsistencyCheck.get();
      return StartupConsistencyCheck.complete(inconsistentUris);
    } catch (Exception e) {
      LOG.warn("Failed to complete start up consistency check.", e);
      return StartupConsistencyCheck.failed();
    }
  }

  @Override
  public long getFileId(AlluxioURI path) throws AccessControlException, UnavailableException {
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree.lockInodePath(path, LockPattern.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExist(rpcContext, inodePath, LoadMetadataContext
          .mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(true)));
      mInodeTree.ensureFullInodePath(inodePath);
      return inodePath.getInode().getId();
    } catch (InvalidPathException | FileDoesNotExistException e) {
      return IdUtils.INVALID_FILE_ID;
    }
  }

  @Override
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, LockPattern.READ)) {
      return getFileInfoInternal(inodePath);
    }
  }

  @Override
  public FileInfo getFileInfo(AlluxioURI path, GetStatusContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    Metrics.GET_FILE_INFO_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, context.getOptions().getCommonOptions(), LockPattern.READ);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("getFileInfo", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Possible ufs sync.
      if (syncMetadata(rpcContext, inodePath, lockingScheme, DescendantType.ONE)) {
        // If synced, do not load metadata.
        context.getOptions().setLoadMetadataType(LoadMetadataPType.NEVER);
      }

      // If the file already exists, then metadata does not need to be loaded,
      // otherwise load metadata.
      if (!inodePath.fullPathExists()) {
        checkLoadMetadataOptions(context.getOptions().getLoadMetadataType(), inodePath.getUri());
        loadMetadataIfNotExist(rpcContext, inodePath,
            LoadMetadataContext.mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(true)
                .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                    .setTtl(context.getOptions().getCommonOptions().getTtl())
                    .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()))));
        ensureFullPathAndUpdateCache(inodePath);
      }
      FileInfo fileInfo = getFileInfoInternal(inodePath);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return fileInfo;
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given inode
   */
  private FileInfo getFileInfoInternal(LockedInodePath inodePath)
      throws FileDoesNotExistException, UnavailableException {
    Inode inode = inodePath.getInode();
    AlluxioURI uri = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(uri.toString());
    if (fileInfo.isFolder()) {
      fileInfo.setLength(inode.asDirectory().getChildCount());
    }
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    fileInfo.setInAlluxioPercentage(getInAlluxioPercentage(inode));
    if (inode.isFile()) {
      try {
        fileInfo.setFileBlockInfos(getFileBlockInfoListInternal(inodePath));
      } catch (InvalidPathException e) {
        throw new FileDoesNotExistException(e.getMessage(), e);
      }
    }
    MountTable.Resolution resolution;
    try {
      resolution = mMountTable.resolve(uri);
    } catch (InvalidPathException e) {
      throw new FileDoesNotExistException(e.getMessage(), e);
    }
    AlluxioURI resolvedUri = resolution.getUri();
    fileInfo.setUfsPath(resolvedUri.toString());
    fileInfo.setMountId(resolution.getMountId());
    Metrics.FILE_INFOS_GOT.inc();
    return fileInfo;
  }

  @Override
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, LockPattern.READ)) {
      return inodePath.getInode().getPersistenceState();
    }
  }

  @Override
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException {
    Metrics.GET_FILE_INFO_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, context.getOptions().getCommonOptions(), LockPattern.READ);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("listStatus", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }

      DescendantType descendantType = context.getOptions().getRecursive() ? DescendantType.ALL
          : DescendantType.ONE;
      // Possible ufs sync.
      if (syncMetadata(rpcContext, inodePath, lockingScheme, descendantType)) {
        // If synced, do not load metadata.
        context.getOptions().setLoadMetadataType(LoadMetadataPType.NEVER);
      }

      DescendantType loadDescendantType;
      if (context.getOptions().getLoadMetadataType() == LoadMetadataPType.NEVER) {
        loadDescendantType = DescendantType.NONE;
      } else if (context.getOptions().getRecursive()) {
        loadDescendantType = DescendantType.ALL;
      } else {
        loadDescendantType = DescendantType.ONE;
      }
      // load metadata for 1 level of descendants, or all descendants if recursive
      LoadMetadataContext loadMetadataContext =
          LoadMetadataContext.mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(true)
              .setLoadDescendantType(GrpcUtils.toProto(loadDescendantType))
              .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                  .setTtl(context.getOptions().getCommonOptions().getTtl())
                  .setTtlAction(context.getOptions().getCommonOptions().getTtlAction())));
      Inode inode;
      if (inodePath.fullPathExists()) {
        inode = inodePath.getInode();
        if (inode.isDirectory()
            && context.getOptions().getLoadMetadataType() != LoadMetadataPType.ALWAYS) {
          InodeDirectory inodeDirectory = inode.asDirectory();

          boolean isLoaded = inodeDirectory.isDirectChildrenLoaded();
          if (context.getOptions().getRecursive()) {
            isLoaded = areDescendantsLoaded(inodeDirectory);
          }
          if (isLoaded) {
            // no need to load again.
            loadMetadataContext.getOptions().setLoadDescendantType(LoadDescendantPType.NONE);
          }
        }
      } else {
        checkLoadMetadataOptions(context.getOptions().getLoadMetadataType(), inodePath.getUri());
      }

      loadMetadataIfNotExist(rpcContext, inodePath, loadMetadataContext);
      ensureFullPathAndUpdateCache(inodePath);
      inode = inodePath.getInode();
      auditContext.setSrcInode(inode);
      List<FileInfo> ret = new ArrayList<>();
      DescendantType descendantTypeForListStatus = (context.getOptions().getRecursive())
          ? DescendantType.ALL : DescendantType.ONE;
      listStatusInternal(inodePath, auditContext, descendantTypeForListStatus, ret);

      // If we are listing the status of a directory, we remove the directory info that we inserted
      if (inode.isDirectory() && ret.size() >= 1) {
        ret.remove(ret.size() - 1);
      }

      auditContext.setSucceeded(true);
      Metrics.FILE_INFOS_GOT.inc();
      return ret;
    }
  }

  /**
   * Lists the status of the path in {@link LockedInodePath}, possibly recursively depending on
   * the descendantType. The result is returned via a list specified by statusList, in postorder
   * traversal order.
   *
   * @param currInodePath the inode path to find the status
   * @param auditContext the audit context to return any access exceptions
   * @param descendantType if the currInodePath is a directory, how many levels of its descendant
   *                       should be returned
   * @param statusList To be populated with the status of the files and directories requested
   */
  private void listStatusInternal(LockedInodePath currInodePath, AuditContext auditContext,
      DescendantType descendantType, List<FileInfo> statusList)
      throws FileDoesNotExistException, UnavailableException,
      AccessControlException, InvalidPathException {
    Inode inode = currInodePath.getInode();
    if (inode.isDirectory() && descendantType != DescendantType.NONE) {
      try {
        // TODO(david): Return the error message when we do not have permission
        mPermissionChecker.checkPermission(Mode.Bits.EXECUTE, currInodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        if (descendantType == DescendantType.ALL) {
          return;
        } else {
          throw e;
        }
      }
      DescendantType nextDescendantType = (descendantType == DescendantType.ALL)
          ? DescendantType.ALL : DescendantType.NONE;
      // This is to generate a parsed child path components to be passed to lockChildPath
      String [] childComponentsHint = null;
      for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
        if (childComponentsHint == null) {
          String[] parentComponents = PathUtils.getPathComponents(currInodePath.getUri().getPath());
          childComponentsHint = new String[parentComponents.length + 1];
          System.arraycopy(parentComponents, 0, childComponentsHint, 0, parentComponents.length);
        }
        // TODO(david): Make extending InodePath more efficient
        childComponentsHint[childComponentsHint.length - 1] = child.getName();

        try (LockedInodePath childInodePath =
            currInodePath.lockChild(child, LockPattern.READ, childComponentsHint)) {
          listStatusInternal(childInodePath, auditContext, nextDescendantType, statusList);
        } catch (InvalidPathException | FileDoesNotExistException e) {
          LOG.debug("Path \"{0}\" is invalid, has been ignored.",
              PathUtils.concatPath("/", childComponentsHint));
        }
      }
    }
    statusList.add(getFileInfoInternal(currInodePath));
  }

  /**
   * Checks the {@link LoadMetadataPType} to determine whether or not to proceed in loading
   * metadata. This method assumes that the path does not exist in Alluxio namespace, and will
   * throw an exception if metadata should not be loaded.
   *
   * @param loadMetadataType the {@link LoadMetadataPType} to check
   * @param path the path that does not exist in Alluxio namespace (used for exception message)
   */
  private void checkLoadMetadataOptions(LoadMetadataPType loadMetadataType, AlluxioURI path)
          throws FileDoesNotExistException {
    if (loadMetadataType == LoadMetadataPType.NEVER || (loadMetadataType == LoadMetadataPType.ONCE
            && mUfsAbsentPathCache.isAbsent(path))) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
  }

  private boolean areDescendantsLoaded(InodeDirectoryView inode) {
    if (!inode.isDirectChildrenLoaded()) {
      return false;
    }
    for (Inode child : mInodeStore.getChildren(inode)) {
      if (child.isDirectory()) {
        if (!areDescendantsLoaded(child.asDirectory())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks to see if the entire path exists in Alluxio. Updates the absent cache if it does not
   * exist.
   *
   * @param inodePath the path to ensure
   */
  private void ensureFullPathAndUpdateCache(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException {
    boolean exists = false;
    try {
      mInodeTree.ensureFullInodePath(inodePath);
      exists = true;
    } finally {
      if (!exists) {
        mUfsAbsentPathCache.process(inodePath.getUri(), inodePath.getInodeList());
      }
    }
  }

  @Override
  public FileSystemMasterView getFileSystemMasterView() {
    return new FileSystemMasterView(this);
  }

  @Override
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException {
    LockingScheme lockingScheme =
        createLockingScheme(path, context.getOptions().getCommonOptions(), LockPattern.READ);
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath parent =
             mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("checkConsistency", path, null, parent.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, parent);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Possible ufs sync.
      syncMetadata(rpcContext, parent, lockingScheme, DescendantType.ALL);
      checkConsistencyRecursive(parent, inconsistentUris);

      auditContext.setSucceeded(true);
    }
    return inconsistentUris;
  }

  private void checkConsistencyRecursive(LockedInodePath inodePath,
      List<AlluxioURI> inconsistentUris) throws IOException, FileDoesNotExistException {
    Inode inode = inodePath.getInode();
    try {
      if (!checkConsistencyInternal(inodePath)) {
        inconsistentUris.add(inodePath.getUri());
      }
      if (inode.isDirectory()) {
        InodeDirectory inodeDir = inode.asDirectory();
        for (Inode child : mInodeStore.getChildren(inodeDir)) {
          try (LockedInodePath childPath = inodePath.lockChild(child, LockPattern.READ)) {
            checkConsistencyRecursive(childPath, inconsistentUris);
          }
        }
      }
    } catch (InvalidPathException e) {
      LOG.debug("Path \"{0}\" is invalid, has been ignored.",
          PathUtils.concatPath(inodePath.getUri().getPath()));
    }
  }

  /**
   * Checks if a path is consistent between Alluxio and the underlying storage.
   * <p>
   * A path without a backing under storage is always consistent.
   * <p>
   * A not persisted path is considered consistent if:
   * 1. It does not shadow an object in the underlying storage.
   * <p>
   * A persisted path is considered consistent if:
   * 1. An equivalent object exists for its under storage path.
   * 2. The metadata of the Alluxio and under storage object are equal.
   *
   * @param inodePath the path to check. This must exist and be read-locked
   * @return true if the path is consistent, false otherwise
   */
  private boolean checkConsistencyInternal(LockedInodePath inodePath) throws InvalidPathException,
      IOException {
    Inode inode;
    try {
      inode = inodePath.getInode();
    } catch (FileDoesNotExistException e) {
      throw new RuntimeException(e); // already checked existence when creating the inodePath
    }
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      String ufsPath = resolution.getUri().getPath();
      if (ufs == null) {
        return true;
      }
      if (!inode.isPersisted()) {
        return !ufs.exists(ufsPath);
      }
      // TODO(calvin): Evaluate which other metadata fields should be validated.
      if (inode.isDirectory()) {
        return ufs.isDirectory(ufsPath);
      } else {
        return ufs.isFile(ufsPath)
            && ufs.getFileStatus(ufsPath).getContentLength() == inode.asFile().getLength();
      }
    }
  }

  @Override
  public void completeFile(AlluxioURI path, CompleteFileContext context)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException,
      UnavailableException {
    Metrics.COMPLETE_FILE_OPS.inc();
    // No need to syncMetadata before complete.
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, LockPattern.WRITE_INODE);
         FileSystemMasterAuditContext auditContext =
             createAuditContext("completeFile", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Even readonly mount points should be able to complete a file, for UFS reads in CACHE mode.
      completeFileInternal(rpcContext, inodePath, context);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to complete
   * @param context the method context
   */
  private void completeFileInternal(RpcContext rpcContext, LockedInodePath inodePath,
      CompleteFileContext context)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException, UnavailableException {
    Inode inode = inodePath.getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_MUST_BE_FILE.getMessage(inodePath.getUri()));
    }

    InodeFile fileInode = inode.asFile();
    List<Long> blockIdList = fileInode.getBlockIds();
    List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(blockIdList);
    if (!fileInode.isPersisted() && blockInfoList.size() != blockIdList.size()) {
      throw new BlockInfoException("Cannot complete a file without all the blocks committed");
    }

    // Iterate over all file blocks committed to Alluxio, computing the length and verify that all
    // the blocks (except the last one) is the same size as the file block size.
    long inAlluxioLength = 0;
    long fileBlockSize = fileInode.getBlockSizeBytes();
    for (int i = 0; i < blockInfoList.size(); i++) {
      BlockInfo blockInfo = blockInfoList.get(i);
      inAlluxioLength += blockInfo.getLength();
      if (i < blockInfoList.size() - 1 && blockInfo.getLength() != fileBlockSize) {
        throw new BlockInfoException(
            "Block index " + i + " has a block size smaller than the file block size (" + fileInode
                .getBlockSizeBytes() + ")");
      }
    }

    // If the file is persisted, its length is determined by UFS. Otherwise, its length is
    // determined by its size in Alluxio.
    long length = fileInode.isPersisted() ? context.getOptions().getUfsLength() : inAlluxioLength;

    String ufsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
    if (fileInode.isPersisted()) {
      UfsStatus ufsStatus = context.getUfsStatus();
      // Retrieve the UFS fingerprint for this file.
      MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
      AlluxioURI resolvedUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        if (ufsStatus == null) {
          ufsFingerprint = ufs.getFingerprint(resolvedUri.toString());
        } else {
          ufsFingerprint = Fingerprint.create(ufs.getUnderFSType(), ufsStatus).serialize();
        }
      }
    }

    completeFileInternal(rpcContext, inodePath, length, context.getOperationTimeMs(),
        ufsFingerprint);
  }

  /**
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to complete
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param ufsFingerprint the ufs fingerprint
   */
  private void completeFileInternal(RpcContext rpcContext, LockedInodePath inodePath, long length,
      long opTimeMs, String ufsFingerprint)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException, UnavailableException {
    Preconditions.checkState(inodePath.getLockPattern().isWrite());

    InodeFile inode = inodePath.getInodeFile();
    if (inode.isCompleted() && inode.getLength() != Constants.UNKNOWN_SIZE) {
      throw new FileAlreadyCompletedException("File " + getName() + " has already been completed.");
    }
    if (length < 0 && length != Constants.UNKNOWN_SIZE) {
      throw new InvalidFileSizeException(
          "File " + inode.getName() + " cannot have negative length: " + length);
    }
    Builder entry = UpdateInodeFileEntry.newBuilder()
        .setId(inode.getId())
        .setCompleted(true)
        .setLength(length);

    if (length == Constants.UNKNOWN_SIZE) {
      // TODO(gpang): allow unknown files to be multiple blocks.
      // If the length of the file is unknown, only allow 1 block to the file.
      length = inode.getBlockSizeBytes();
    }
    int sequenceNumber = 0;
    long remainingBytes = length;
    while (remainingBytes > 0) {
      entry.addSetBlocks(BlockId.createBlockId(inode.getBlockContainerId(), sequenceNumber));
      remainingBytes -= Math.min(remainingBytes, inode.getBlockSizeBytes());
      sequenceNumber++;
    }

    if (inode.isPersisted()) {
      // Commit all the file blocks (without locations) so the metadata for the block exists.
      long currLength = length;
      for (long blockId : entry.getSetBlocksList()) {
        long blockSize = Math.min(currLength, inode.getBlockSizeBytes());
        mBlockMaster.commitBlockInUFS(blockId, blockSize);
        currLength -= blockSize;
      }
      // The path exists in UFS, so it is no longer absent
      mUfsAbsentPathCache.processExisting(inodePath.getUri());
    }

    // We could introduce a concept of composite entries, so that these two entries could
    // be applied in a single call to applyAndJournal.
    mInodeTree.updateInode(rpcContext, UpdateInodeEntry.newBuilder()
        .setId(inode.getId())
        .setUfsFingerprint(ufsFingerprint)
        .setLastModificationTimeMs(opTimeMs)
        .setOverwriteModificationTime(true)
        .build());
    mInodeTree.updateInodeFile(rpcContext, entry.build());

    Metrics.FILES_COMPLETED.inc();
  }

  @Override
  public FileInfo createFile(AlluxioURI path, CreateFileContext context)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    Metrics.CREATE_FILES_OPS.inc();
    LockingScheme lockingScheme = createLockingScheme(path, context.getOptions().getCommonOptions(),
            LockPattern.WRITE_EDGE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
            createAuditContext("createFile", path, null, inodePath.getParentInodeOrNull())) {
      if (context.getOptions().getRecursive()) {
        auditContext.setSrcInode(inodePath.getLastExistingInode());
      }
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme, DescendantType.ONE);

      mMountTable.checkUnderWritableMountPoint(path);
      if (context.getPersisted()) {
        // Check if ufs is writable
        checkUfsMode(path, OperationType.WRITE);
      }
      createFileInternal(rpcContext, inodePath, context);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return getFileInfoInternal(inodePath);
    }
  }

  /**
   * @param rpcContext the rpc context
   * @param inodePath the path to be created
   * @param context the method context
   * @return the list of created inodes
   */
  List<Inode> createFileInternal(RpcContext rpcContext, LockedInodePath inodePath,
                                 CreateFileContext context)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      FileDoesNotExistException {
    if (mWhitelist.inList(inodePath.getUri().toString())) {
      context.setCacheable(true);
    }
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode> created = mInodeTree.createPath(rpcContext, inodePath, context);

    if (context.isPersisted()) {
      // The path exists in UFS, so it is no longer absent. The ancestors exist in UFS, but the
      // actual file does not exist in UFS yet.
      mUfsAbsentPathCache.processExisting(inodePath.getUri().getParent());
    }

    Metrics.FILES_CREATED.inc();
    return created;
  }

  @Override
  public long getNewBlockIdForFile(AlluxioURI path) throws FileDoesNotExistException,
      InvalidPathException, AccessControlException, UnavailableException {
    Metrics.GET_NEW_BLOCK_OPS.inc();
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, LockPattern.WRITE_INODE);
         FileSystemMasterAuditContext auditContext =
            createAuditContext("getNewBlockIdForFile", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      Metrics.NEW_BLOCKS_GOT.inc();

      long blockId = mInodeTree.newBlock(rpcContext, NewBlockEntry.newBuilder()
          .setId(inodePath.getInode().getId())
          .build());
      auditContext.setSucceeded(true);
      return blockId;
    }
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() {
    SortedMap<String, MountPointInfo> mountPoints = new TreeMap<>();
    for (Map.Entry<String, MountInfo> mountPoint : mMountTable.getMountTable().entrySet()) {
      mountPoints.put(mountPoint.getKey(), getMountPointInfo(mountPoint.getValue()));
    }
    return mountPoints;
  }

  @Override
  public MountPointInfo getMountPointInfo(AlluxioURI path) throws InvalidPathException {
    if (!mMountTable.isMountPoint(path)) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_BE_MOUNT_POINT.getMessage(path));
    }
    return getMountPointInfo(mMountTable.getMountTable().get(path.toString()));
  }

  /**
   * Gets the mount point information from a mount information.
   *
   * @param mountInfo the mount information to transform
   * @return the mount point information
   */
  private MountPointInfo getMountPointInfo(MountInfo mountInfo) {
    MountPointInfo info = mountInfo.toMountPointInfo();
    try (CloseableResource<UnderFileSystem> ufsResource =
             mUfsManager.get(mountInfo.getMountId()).acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      info.setUfsType(ufs.getUnderFSType());
      try {
        info.setUfsCapacityBytes(
            ufs.getSpace(info.getUfsUri(), UnderFileSystem.SpaceType.SPACE_TOTAL));
      } catch (IOException e) {
        LOG.warn("Cannot get total capacity of {}", info.getUfsUri(), e);
      }
      try {
        info.setUfsUsedBytes(
            ufs.getSpace(info.getUfsUri(), UnderFileSystem.SpaceType.SPACE_USED));
      } catch (IOException e) {
        LOG.warn("Cannot get used capacity of {}", info.getUfsUri(), e);
      }
    } catch (UnavailableException | NotFoundException e) {
      // We should never reach here
      LOG.error("No UFS cached for {}", info, e);
    }
    return info;
  }

  @Override
  public long getInodeCount() {
    return mInodeTree.getInodeCount();
  }

  @Override
  public int getNumberOfPinnedFiles() {
    return mInodeTree.getPinnedSize();
  }

  @Override
  public void delete(AlluxioURI path, DeleteContext context)
      throws IOException, FileDoesNotExistException, DirectoryNotEmptyException,
      InvalidPathException, AccessControlException {
    Metrics.DELETE_PATHS_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, context.getOptions().getCommonOptions(), LockPattern.WRITE_EDGE);
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath =
            mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
        FileSystemMasterAuditContext auditContext =
            createAuditContext("delete", path, null, inodePath.getInodeOrNull())) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      if (context.getOptions().getRecursive()) {
        try {
          List<String> failedChildren = new ArrayList<>();
          for (LockedInodePath childPath : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
            try {
              mPermissionChecker.checkPermission(Mode.Bits.WRITE, childPath);
            } catch (AccessControlException e) {
              failedChildren.add(e.getMessage());
            }
          }
          if (failedChildren.size() > 0) {
            throw new AccessControlException(ExceptionMessage.DELETE_FAILED_DIR_CHILDREN
                .getMessage(path, StringUtils.join(failedChildren, ",")));
          }
        } catch (AccessControlException e) {
          auditContext.setAllowed(false);
          throw e;
        }
      }
      mMountTable.checkUnderWritableMountPoint(path);
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme,
          context.getOptions().getRecursive() ? DescendantType.ALL : DescendantType.ONE);
      if (!inodePath.fullPathExists()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }

      deleteInternal(rpcContext, inodePath, context);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Implements file deletion.
   * <p>
   * This method does not delete blocks. Instead, it returns deleted inodes so that their blocks can
   * be deleted after the inode deletion journal entry has been written. We cannot delete blocks
   * earlier because the inode deletion may fail, leaving us with inode containing deleted blocks.
   *
   * @param rpcContext the rpc context
   * @param inodePath the file {@link LockedInodePath}
   * @param deleteContext the method optitions
   */
  @VisibleForTesting
  public void deleteInternal(RpcContext rpcContext, LockedInodePath inodePath,
      DeleteContext deleteContext) throws FileDoesNotExistException, IOException,
      DirectoryNotEmptyException, InvalidPathException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Alluxio and UFS.
    if (!inodePath.fullPathExists()) {
      return;
    }
    long opTimeMs = System.currentTimeMillis();
    Inode inode = inodePath.getInode();
    if (inode == null) {
      return;
    }

    boolean recursive = deleteContext.getOptions().getRecursive();
    if (inode.isDirectory() && !recursive && mInodeStore.hasChildren(inode.asDirectory())) {
      // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
      // true
      throw new DirectoryNotEmptyException(ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE,
          inode.getName());
    }
    if (mInodeTree.isRootId(inode.getId())) {
      // The root cannot be deleted.
      throw new InvalidPathException(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    }

    // Inodes for which deletion will be attempted
    List<Pair<AlluxioURI, LockedInodePath>> inodesToDelete = new ArrayList<>();

    // Add root of sub-tree to delete
    inodesToDelete.add(new Pair<>(inodePath.getUri(), inodePath));

    for (LockedInodePath childPath : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
      inodesToDelete.add(new Pair<>(mInodeTree.getPath(childPath.getInode()), childPath));
    }
    // Prepare to delete persisted inodes
    UfsDeleter ufsDeleter = NoopUfsDeleter.INSTANCE;
    if (!deleteContext.getOptions().getAlluxioOnly()) {
      ufsDeleter = new SafeUfsDeleter(mMountTable, mInodeStore, inodesToDelete,
          deleteContext.getOptions().build());
    }

    // Inodes to delete from tree after attempting to delete from UFS
    List<Pair<AlluxioURI, LockedInodePath>> revisedInodesToDelete = new ArrayList<>();
    // Inodes that are not safe for recursive deletes
    Set<Long> unsafeInodes = new HashSet<>();
    // Alluxio URIs (and the reason for failure) which could not be deleted
    List<Pair<String, String>> failedUris = new ArrayList<>();

    // We go through each inode, removing it from its parent set and from mDelInodes. If it's a
    // file, we deal with the checkpoints and blocks as well.
    for (int i = inodesToDelete.size() - 1; i >= 0; i--) {
      Pair<AlluxioURI, LockedInodePath> inodePairToDelete = inodesToDelete.get(i);
      AlluxioURI alluxioUriToDelete = inodePairToDelete.getFirst();
      Inode inodeToDelete = inodePairToDelete.getSecond().getInode();

      String failureReason = null;
      if (unsafeInodes.contains(inodeToDelete.getId())) {
        failureReason = ExceptionMessage.DELETE_FAILED_DIR_NONEMPTY.getMessage();
      } else if (inodeToDelete.isPersisted()) {
        // If this is a mount point, we have deleted all the children and can unmount it
        // TODO(calvin): Add tests (ALLUXIO-1831)
        if (mMountTable.isMountPoint(alluxioUriToDelete)) {
          mMountTable.delete(rpcContext, alluxioUriToDelete);
        } else {
          if (!deleteContext.getOptions().getAlluxioOnly()) {
            try {
              checkUfsMode(alluxioUriToDelete, OperationType.WRITE);
              // Attempt to delete node if all children were deleted successfully
              ufsDeleter.delete(alluxioUriToDelete, inodeToDelete);
            } catch (AccessControlException e) {
              // In case ufs is not writable, we will still attempt to delete other entries
              // if any as they may be from a different mount point
              LOG.warn(e.getMessage());
              failureReason = e.getMessage();
            } catch (IOException e) {
              LOG.warn(e.getMessage());
              failureReason = e.getMessage();
            }
          }
        }
      }
      if (failureReason == null) {
        if (inodeToDelete.isFile()) {
          long fileId = inodeToDelete.getId();
          // Remove the file from the set of files to persist.
          mPersistRequests.remove(fileId);
          // Cancel any ongoing jobs.
          PersistJob job = mPersistJobs.get(fileId);
          if (job != null) {
            job.setCancelState(PersistJob.CancelState.TO_BE_CANCELED);
          }
        }
        revisedInodesToDelete.add(new Pair<>(alluxioUriToDelete, inodePairToDelete.getSecond()));
      } else {
        unsafeInodes.add(inodeToDelete.getId());
        // Propagate 'unsafe-ness' to parent as one of its descendants can't be deleted
        unsafeInodes.add(inodeToDelete.getParentId());
        failedUris.add(new Pair<>(alluxioUriToDelete.toString(), failureReason));
      }
    }

    MountTable.Resolution resolution = mSyncManager.resolveSyncPoint(inodePath.getUri());
    if (resolution != null) {
      mSyncManager.stopSyncInternal(inodePath.getUri(), resolution);
    }

    // Delete Inodes
    for (Pair<AlluxioURI, LockedInodePath> delInodePair : revisedInodesToDelete) {
      LockedInodePath tempInodePath = delInodePair.getSecond();
      mInodeTree.deleteInode(rpcContext, tempInodePath, opTimeMs);
    }

    if (!failedUris.isEmpty()) {
      Collection<String> messages = failedUris.stream()
          .map(pair -> String.format("%s (%s)", pair.getFirst(), pair.getSecond()))
          .collect(Collectors.toList());
      throw new FailedPreconditionException(
          ExceptionMessage.DELETE_FAILED_UFS.getMessage(StringUtils.join(messages, ", ")));
    }

    Metrics.PATHS_DELETED.inc(inodesToDelete.size());
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException {
    Metrics.GET_FILE_BLOCK_INFO_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, LockPattern.READ);
         FileSystemMasterAuditContext auditContext =
            createAuditContext("getFileBlockInfoList", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      List<FileBlockInfo> ret = getFileBlockInfoListInternal(inodePath);
      Metrics.FILE_BLOCK_INFOS_GOT.inc();
      auditContext.setSucceeded(true);
      return ret;
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given inode
   */
  private List<FileBlockInfo> getFileBlockInfoListInternal(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException, UnavailableException {
    InodeFile file = inodePath.getInodeFile();
    List<BlockInfo> blockInfoList = mBlockMaster.getBlockInfoList(file.getBlockIds());

    List<FileBlockInfo> ret = new ArrayList<>();
    for (BlockInfo blockInfo : blockInfoList) {
      ret.add(generateFileBlockInfo(inodePath, blockInfo));
    }
    return ret;
  }

  /**
   * Generates a {@link FileBlockInfo} object from internal metadata. This adds file information to
   * the block, such as the file offset, and additional UFS locations for the block.
   *
   * @param inodePath the file the block is a part of
   * @param blockInfo the {@link BlockInfo} to generate the {@link FileBlockInfo} from
   * @return a new {@link FileBlockInfo} for the block
   */
  private FileBlockInfo generateFileBlockInfo(LockedInodePath inodePath, BlockInfo blockInfo)
      throws FileDoesNotExistException {
    InodeFile file = inodePath.getInodeFile();
    FileBlockInfo fileBlockInfo = new FileBlockInfo();
    fileBlockInfo.setBlockInfo(blockInfo);
    fileBlockInfo.setUfsLocations(new ArrayList<>());

    // The sequence number part of the block id is the block index.
    long offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.getBlockId());
    fileBlockInfo.setOffset(offset);

    if (fileBlockInfo.getBlockInfo().getLocations().isEmpty() && file.isPersisted()) {
      // No alluxio locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      long blockId = fileBlockInfo.getBlockInfo().getBlockId();
      List<String> locations = mUfsBlockLocationCache.get(blockId, inodePath.getUri(),
          fileBlockInfo.getOffset());
      if (locations != null) {
        fileBlockInfo.setUfsLocations(locations);
      }
    }
    return fileBlockInfo;
  }

  /**
   * Returns whether the inodeFile is fully in Alluxio or not. The file is fully in Alluxio only if
   * all the blocks of the file are in Alluxio, in other words, the in-Alluxio percentage is 100.
   *
   * @return true if the file is fully in Alluxio, false otherwise
   */
  private boolean isFullyInAlluxio(InodeFile inode) throws UnavailableException {
    return getInAlluxioPercentage(inode) == 100;
  }

  /**
   * Returns whether the inodeFile is fully in memory or not. The file is fully in memory only if
   * all the blocks of the file are in memory, in other words, the in-memory percentage is 100.
   *
   * @return true if the file is fully in Alluxio, false otherwise
   */
  private boolean isFullyInMemory(InodeFile inode) throws UnavailableException {
    return getInMemoryPercentage(inode) == 100;
  }

  @Override
  public List<AlluxioURI> getInAlluxioFiles() throws UnavailableException {
    List<AlluxioURI> files = new ArrayList<>();
    LockedInodePath rootPath;
    try {
      rootPath =
          mInodeTree.lockFullInodePath(new AlluxioURI(AlluxioURI.SEPARATOR), LockPattern.READ);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      // Root should always exist.
      throw new RuntimeException(e);
    }

    try (LockedInodePath inodePath = rootPath) {
      getInAlluxioFilesInternal(inodePath, files);
    }
    return files;
  }

  @Override
  public List<AlluxioURI> getInMemoryFiles() throws UnavailableException {
    List<AlluxioURI> files = new ArrayList<>();
    LockedInodePath rootPath;
    try {
      rootPath =
          mInodeTree.lockFullInodePath(new AlluxioURI(AlluxioURI.SEPARATOR), LockPattern.READ);
    } catch (FileDoesNotExistException | InvalidPathException e) {
      // Root should always exist.
      throw new RuntimeException(e);
    }

    try (LockedInodePath inodePath = rootPath) {
      getInMemoryFilesInternal(inodePath, files);
    }
    return files;
  }

  /**
   * Adds in-Alluxio files to the array list passed in. This method assumes the inode passed in is
   * already read locked.
   *
   * @param inodePath the inode path to search
   * @param files the list to accumulate the results in
   */
  private void getInAlluxioFilesInternal(LockedInodePath inodePath, List<AlluxioURI> files)
      throws UnavailableException {
    Inode inode = inodePath.getInodeOrNull();
    if (inode == null) {
      return;
    }

    if (inode.isFile()) {
      if (isFullyInAlluxio(inode.asFile())) {
        files.add(inodePath.getUri());
      }
    } else {
      // This inode is a directory.
      for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
        try (LockedInodePath childPath = inodePath.lockChild(child, LockPattern.READ)) {
          getInAlluxioFilesInternal(childPath, files);
        } catch (InvalidPathException e) {
          // Inode is no longer a child, continue.
          continue;
        }
      }
    }
  }

  /**
   * Adds in-memory files to the array list passed in. This method assumes the inode passed in is
   * already read locked.
   *
   * @param inodePath the inode path to search
   * @param files the list to accumulate the results in
   */
  private void getInMemoryFilesInternal(LockedInodePath inodePath, List<AlluxioURI> files)
      throws UnavailableException {
    Inode inode = inodePath.getInodeOrNull();
    if (inode == null) {
      return;
    }

    if (inode.isFile()) {
      if (isFullyInMemory(inode.asFile())) {
        files.add(inodePath.getUri());
      }
    } else {
      // This inode is a directory.
      for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
        try (LockedInodePath childPath = inodePath.lockChild(child, LockPattern.READ)) {
          getInMemoryFilesInternal(childPath, files);
        } catch (InvalidPathException e) {
          // Inode is no longer a child, continue.
          continue;
        }
      }
    }
  }

  /**
   * Gets the in-memory percentage of an Inode. For a file that has all blocks in Alluxio, it
   * returns 100; for a file that has no block in memory, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in memory percentage
   */
  private int getInMemoryPercentage(Inode inode) throws UnavailableException {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = inode.asFile();

    long length = inodeFile.getLength();
    if (length == 0) {
      return 100;
    }

    long inMemoryLength = 0;
    for (BlockInfo info : mBlockMaster.getBlockInfoList(inodeFile.getBlockIds())) {
      if (isInTopStorageTier(info)) {
        inMemoryLength += info.getLength();
      }
    }
    return (int) (inMemoryLength * 100 / length);
  }

  /**
   * Gets the in-Alluxio percentage of an Inode. For a file that has all blocks in Alluxio, it
   * returns 100; for a file that has no block in Alluxio, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in alluxio percentage
   */
  private int getInAlluxioPercentage(Inode inode) throws UnavailableException {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = inode.asFile();

    long length = inodeFile.getLength();
    if (length == 0) {
      return 100;
    }

    long inAlluxioLength = 0;
    for (BlockInfo info : mBlockMaster.getBlockInfoList(inodeFile.getBlockIds())) {
      if (!info.getLocations().isEmpty()) {
        inAlluxioLength += info.getLength();
      }
    }
    return (int) (inAlluxioLength * 100 / length);
  }

  /**
   * @return true if the given block is in the top storage level in some worker, false otherwise
   */
  private boolean isInTopStorageTier(BlockInfo blockInfo) {
    for (BlockLocation location : blockInfo.getLocations()) {
      if (mBlockMaster.getGlobalStorageTierAssoc().getOrdinal(location.getTierAlias()) == 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public long createDirectory(AlluxioURI path, CreateDirectoryContext context)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    LOG.debug("createDirectory {} ", path);
    Metrics.CREATE_DIRECTORIES_OPS.inc();
    LockingScheme lockingScheme = createLockingScheme(path, context.getOptions().getCommonOptions(),
            LockPattern.WRITE_EDGE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("mkdir", path, null, inodePath.getParentInodeOrNull())) {
      if (context.getOptions().getRecursive()) {
        auditContext.setSrcInode(inodePath.getLastExistingInode());
      }
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme, DescendantType.ONE);

      mMountTable.checkUnderWritableMountPoint(path);
      if (context.getPersisted()) {
        checkUfsMode(path, OperationType.WRITE);
      }
      createDirectoryInternal(rpcContext, inodePath, context);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Implementation of directory creation for a given path.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path of the directory
   * @param context method context
   * @return a list of created inodes
   */
  private List<Inode> createDirectoryInternal(RpcContext rpcContext, LockedInodePath inodePath,
      CreateDirectoryContext context) throws InvalidPathException, FileAlreadyExistsException,
      IOException, FileDoesNotExistException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    try {
      List<Inode> createResult = mInodeTree.createPath(rpcContext, inodePath, context);
      InodeDirectory inodeDirectory = inodePath.getInode().asDirectory();

      String ufsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
      if (inodeDirectory.isPersisted()) {
        UfsStatus ufsStatus = context.getUfsStatus();
        // Retrieve the UFS fingerprint for this file.
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        AlluxioURI resolvedUri = resolution.getUri();
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          if (ufsStatus == null) {
            ufsFingerprint = ufs.getFingerprint(resolvedUri.toString());
          } else {
            ufsFingerprint = Fingerprint.create(ufs.getUnderFSType(), ufsStatus).serialize();
          }
        }
      }

      mInodeTree.updateInode(rpcContext, UpdateInodeEntry.newBuilder()
          .setId(inodeDirectory.getId())
          .setUfsFingerprint(ufsFingerprint)
          .build());

      if (context.getPersisted()) {
        // The path exists in UFS, so it is no longer absent.
        mUfsAbsentPathCache.processExisting(inodePath.getUri());
      }

      Metrics.DIRECTORIES_CREATED.inc();
      return createResult;
    } catch (BlockInfoException e) {
      // Since we are creating a directory, the block size is ignored, no such exception should
      // happen.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.RENAME_PATH_OPS.inc();
    LockingScheme srcLockingScheme = createLockingScheme(srcPath,
        context.getOptions().getCommonOptions(), LockPattern.WRITE_EDGE);
    LockingScheme dstLockingScheme = createLockingScheme(dstPath,
        context.getOptions().getCommonOptions(), LockPattern.WRITE_EDGE);
    try (RpcContext rpcContext = createRpcContext();
         InodePathPair inodePathPair = mInodeTree
             .lockInodePathPair(srcLockingScheme.getPath(), srcLockingScheme.getPattern(),
                 dstLockingScheme.getPath(), dstLockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("rename", srcPath, dstPath, null)) {
      LockedInodePath srcInodePath = inodePathPair.getFirst();
      LockedInodePath dstInodePath = inodePathPair.getSecond();
      auditContext.setSrcInode(srcInodePath.getParentInodeOrNull());
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, srcInodePath);
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, dstInodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Possible ufs sync.
      syncMetadata(rpcContext, srcInodePath, srcLockingScheme, DescendantType.ONE);
      syncMetadata(rpcContext, dstInodePath, dstLockingScheme, DescendantType.ONE);

      mMountTable.checkUnderWritableMountPoint(srcPath);
      mMountTable.checkUnderWritableMountPoint(dstPath);
      renameInternal(rpcContext, srcInodePath, dstInodePath, context);
      auditContext.setSrcInode(srcInodePath.getInode()).setSucceeded(true);
      LOG.debug("Renamed {} to {}", srcPath, dstPath);
    }
  }

  /**
   * Renames a file to a destination.
   *
   * @param rpcContext the rpc context
   * @param srcInodePath the source path to rename
   * @param dstInodePath the destination path to rename the file to
   * @param context method options
   */
  private void renameInternal(RpcContext rpcContext, LockedInodePath srcInodePath,
      LockedInodePath dstInodePath, RenameContext context) throws InvalidPathException,
      FileDoesNotExistException, FileAlreadyExistsException, IOException, AccessControlException {
    if (!srcInodePath.fullPathExists()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcInodePath.getUri()));
    }

    Inode srcInode = srcInodePath.getInode();
    // Renaming path to itself is a no-op.
    if (srcInodePath.getUri().equals(dstInodePath.getUri())) {
      return;
    }
    // Renaming the root is not allowed.
    if (srcInodePath.getUri().isRoot()) {
      throw new InvalidPathException(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage());
    }
    if (dstInodePath.getUri().isRoot()) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage());
    }
    // Renaming across mount points is not allowed.
    String srcMount = mMountTable.getMountPoint(srcInodePath.getUri());
    String dstMount = mMountTable.getMountPoint(dstInodePath.getUri());
    if ((srcMount == null && dstMount != null) || (srcMount != null && dstMount == null) || (
        srcMount != null && dstMount != null && !srcMount.equals(dstMount))) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_ACROSS_MOUNTS
          .getMessage(srcInodePath.getUri(), dstInodePath.getUri()));
    }
    // Renaming onto a mount point is not allowed.
    if (mMountTable.isMountPoint(dstInodePath.getUri())) {
      throw new InvalidPathException(
          ExceptionMessage.RENAME_CANNOT_BE_ONTO_MOUNT_POINT.getMessage(dstInodePath.getUri()));
    }
    // Renaming a path to one of its subpaths is not allowed. Check for that, by making sure
    // srcComponents isn't a prefix of dstComponents.
    if (PathUtils.hasPrefix(dstInodePath.getUri().getPath(), srcInodePath.getUri().getPath())) {
      throw new InvalidPathException(ExceptionMessage.RENAME_CANNOT_BE_TO_SUBDIRECTORY
          .getMessage(srcInodePath.getUri(), dstInodePath.getUri()));
    }

    // Get the inodes of the src and dst parents.
    Inode srcParentInode = srcInodePath.getParentInodeDirectory();
    if (!srcParentInode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(srcInodePath.getUri()));
    }
    Inode dstParentInode = dstInodePath.getParentInodeDirectory();
    if (!dstParentInode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(dstInodePath.getUri()));
    }

    // Make sure destination path does not exist
    if (dstInodePath.fullPathExists()) {
      throw new FileAlreadyExistsException(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(dstInodePath.getUri()));
    }

    // Now we remove srcInode from its parent and insert it into dstPath's parent
    renameInternal(rpcContext, srcInodePath, dstInodePath, false, context);
  }

  /**
   * Implements renaming.
   *
   * @param rpcContext the rpc context
   * @param srcInodePath the path of the rename source
   * @param dstInodePath the path to the rename destination
   * @param replayed whether the operation is a result of replaying the journal
   * @param context method options
   */
  private void renameInternal(RpcContext rpcContext, LockedInodePath srcInodePath,
      LockedInodePath dstInodePath, boolean replayed, RenameContext context)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {

    // Rename logic:
    // 1. Change the source inode name to the destination name.
    // 2. Insert the source inode into the destination parent.
    // 3. Do UFS operations if necessary.
    // 4. Remove the source inode (reverting the name) from the source parent.
    // 5. Set the last modification times for both source and destination parent inodes.

    Inode srcInode = srcInodePath.getInode();
    AlluxioURI srcPath = srcInodePath.getUri();
    AlluxioURI dstPath = dstInodePath.getUri();
    InodeDirectory srcParentInode = srcInodePath.getParentInodeDirectory();
    InodeDirectory dstParentInode = dstInodePath.getParentInodeDirectory();
    String srcName = srcPath.getName();
    String dstName = dstPath.getName();

    LOG.debug("Renaming {} to {}", srcPath, dstPath);
    if (dstInodePath.fullPathExists()) {
      throw new InvalidPathException("Destination path: " + dstPath + " already exists.");
    }

    mInodeTree.rename(rpcContext, RenameEntry.newBuilder()
        .setId(srcInode.getId())
        .setOpTimeMs(context.getOperationTimeMs())
        .setNewParentId(dstParentInode.getId())
        .setNewName(dstName)
        .build());

    // 3. Do UFS operations if necessary.
    // If the source file is persisted, rename it in the UFS.
    try {
      if (!replayed && srcInode.isPersisted()) {
        // Check if ufs is writable
        checkUfsMode(srcPath, OperationType.WRITE);
        checkUfsMode(dstPath, OperationType.WRITE);

        MountTable.Resolution resolution = mMountTable.resolve(srcPath);
        // Persist ancestor directories from top to the bottom. We cannot use recursive create
        // parents here because the permission for the ancestors can be different.

        // inodes from the same mount point as the dst
        Stack<InodeDirectory> sameMountDirs = new Stack<>();
        List<Inode> dstInodeList = dstInodePath.getInodeList();
        for (int i = dstInodeList.size() - 1; i >= 0; i--) {
          // Since dstInodePath is guaranteed not to be a full path, all inodes in the incomplete
          // path are guaranteed to be a directory.
          InodeDirectory dir = dstInodeList.get(i).asDirectory();
          sameMountDirs.push(dir);
          if (dir.isMountPoint()) {
            break;
          }
        }
        while (!sameMountDirs.empty()) {
          InodeDirectory dir = sameMountDirs.pop();
          if (!dir.isPersisted()) {
            mInodeTree.syncPersistExistingDirectory(rpcContext, dir);
          }
        }

        String ufsSrcPath = resolution.getUri().toString();
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          String ufsDstUri = mMountTable.resolve(dstPath).getUri().toString();
          boolean success;
          if (srcInode.isFile()) {
            success = ufs.renameRenamableFile(ufsSrcPath, ufsDstUri);
          } else {
            success = ufs.renameRenamableDirectory(ufsSrcPath, ufsDstUri);
          }
          if (!success) {
            throw new IOException(
                ExceptionMessage.FAILED_UFS_RENAME.getMessage(ufsSrcPath, ufsDstUri));
          }
        }
        // The destination was persisted in ufs.
        mUfsAbsentPathCache.processExisting(dstPath);
      }
    } catch (Throwable t) {
      // On failure, revert changes and throw exception.
      mInodeTree.rename(rpcContext, RenameEntry.newBuilder()
          .setId(srcInode.getId())
          .setOpTimeMs(context.getOperationTimeMs())
          .setNewName(srcName)
          .setNewParentId(srcParentInode.getId())
          .build());
      throw t;
    }

    Metrics.PATHS_RENAMED.inc();
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * @param journalContext the journal context
   * @param inodePath the inode to start the propagation at
   * @return list of inodes which were marked as persisted
   */
  private void propagatePersistedInternal(Supplier<JournalContext> journalContext,
      LockedInodePath inodePath) throws FileDoesNotExistException {
    Inode inode = inodePath.getInode();

    List<Inode> inodes = inodePath.getInodeList();
    // Traverse the inodes from target inode to the root.
    Collections.reverse(inodes);
    // Skip the first, to not examine the target inode itself.
    inodes = inodes.subList(1, inodes.size());

    List<Inode> persistedInodes = new ArrayList<>();
    for (Inode ancestor : inodes) {
      // the path is already locked.
      AlluxioURI path = mInodeTree.getPath(ancestor);
      if (mMountTable.isMountPoint(path)) {
        // Stop propagating the persisted status at mount points.
        break;
      }
      if (ancestor.isPersisted()) {
        // Stop if a persisted directory is encountered.
        break;
      }
      mInodeTree.updateInode(journalContext, UpdateInodeEntry.newBuilder()
          .setId(ancestor.getId())
          .setPersistenceState(PersistenceState.PERSISTED.name())
          .build());
    }
  }

  @Override
  public void free(AlluxioURI path, FreeContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, IOException {
    Metrics.FREE_FILE_OPS.inc();
    // No need to syncMetadata before free.
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, LockPattern.WRITE_INODE);
         FileSystemMasterAuditContext auditContext =
             createAuditContext("free", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      freeInternal(rpcContext, inodePath, context);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Implements free operation.
   *
   * @param rpcContext the rpc context
   * @param inodePath inode of the path to free
   * @param context context to free method
   */
  private void freeInternal(RpcContext rpcContext, LockedInodePath inodePath, FreeContext context)
      throws FileDoesNotExistException, UnexpectedAlluxioException,
      IOException, InvalidPathException, AccessControlException {
    Inode inode = inodePath.getInode();
    if (inode.isDirectory() && !context.getOptions().getRecursive()
        && mInodeStore.hasChildren(inode.asDirectory())) {
      // inode is nonempty, and we don't free a nonempty directory unless recursive is true
      throw new UnexpectedAlluxioException(
          ExceptionMessage.CANNOT_FREE_NON_EMPTY_DIR.getMessage(mInodeTree.getPath(inode)));
    }
    long opTimeMs = System.currentTimeMillis();
    List<Inode> freeInodes = new ArrayList<>();
    freeInodes.add(inode);
    List<LockedInodePath> descendants = mInodeTree.getImplicitlyLockedDescendants(inodePath);
    descendants.add(inodePath);
    for (LockedInodePath descedant : descendants) {
      Inode freeInode = descedant.getInodeOrNull();

      if (freeInode != null && freeInode.isFile()) {
        if (freeInode.getPersistenceState() != PersistenceState.PERSISTED) {
          throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
              .getMessage(mInodeTree.getPath(freeInode)));
        }
        if (freeInode.isPinned()) {
          if (!context.getOptions().getForced()) {
            throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_PINNED_FILE
                .getMessage(mInodeTree.getPath(freeInode)));
          }

          SetAttributeContext setAttributeContext = SetAttributeContext
              .mergeFrom(SetAttributePOptions.newBuilder().setRecursive(false).setPinned(false));
          setAttributeSingleFile(rpcContext, descedant, true, opTimeMs, setAttributeContext);
        }
        // Remove corresponding blocks from workers.
        mBlockMaster.removeBlocks(freeInode.asFile().getBlockIds(), false /* delete */);
      }
    }

    Metrics.FILES_FREED.inc(freeInodes.size());
  }

  @Override
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, LockPattern.READ)) {
      // the path is already locked.
      return mInodeTree.getPath(inodePath.getInode());
    }
  }

  @Override
  public Set<Long> getPinIdList() {
    return mInodeTree.getPinIdSet();
  }

  @Override
  public String getUfsAddress() {
    return ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  }

  @Override
  public UfsInfo getUfsInfo(long mountId) {
    MountInfo info = mMountTable.getMountInfo(mountId);
    if (info == null) {
      return new UfsInfo();
    }
    MountPOptions options = info.getOptions();
    return new UfsInfo().setUri(info.getUfsUri())
        .setMountOptions(MountContext
            .mergeFrom(MountPOptions.newBuilder().putAllProperties(options.getPropertiesMap())
                .setReadOnly(options.getReadOnly()).setShared(options.getShared()))
            .getOptions().build());
  }

  @Override
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  @Override
  public List<Long> getLostFiles() {
    Set<Long> lostFiles = new HashSet<>();
    for (long blockId : mBlockMaster.getLostBlocks()) {
      // the file id is the container id of the block id
      long containerId = BlockId.getContainerId(blockId);
      long fileId = IdUtils.createFileId(containerId);
      lostFiles.add(fileId);
    }
    return new ArrayList<>(lostFiles);
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   *
   * This operation requires users to have WRITE permission on the path
   * and its parent path if path is a file, or WRITE permission on the
   * parent path if path is a directory.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param context the load metadata context
   * @throws AccessControlException if permission checking fails
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws InvalidPathException if invalid path is encountered
   */
  private void loadMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (context.getUfsStatus() == null && !ufs.exists(ufsUri.toString())) {
        // uri does not exist in ufs
        InodeDirectory inode = inodePath.getInode().asDirectory();
        mInodeTree.setDirectChildrenLoaded(rpcContext, inode);
        return;
      }
      boolean isFile;
      if (context.getUfsStatus() != null) {
        isFile = context.getUfsStatus().isFile();
      } else {
        isFile = ufs.isFile(ufsUri.toString());
      }
      if (isFile) {
        loadFileMetadataInternal(rpcContext, inodePath, resolution, context);
      } else {
        loadDirectoryMetadata(rpcContext, inodePath, context);

        if (context.getOptions().getLoadDescendantType() != LoadDescendantPType.NONE) {
          ListOptions listOptions = ListOptions.defaults();
          if (context.getOptions().getLoadDescendantType() == LoadDescendantPType.ALL) {
            listOptions.setRecursive(true);
          } else {
            listOptions.setRecursive(false);
          }
          UfsStatus[] children = ufs.listStatus(ufsUri.toString(), listOptions);
          // children can be null if the pathname does not denote a directory
          // or if the we do not have permission to listStatus on the directory in the ufs.
          if (children == null) {
            throw new IOException("Failed to loadMetadata because ufs can not listStatus at path "
                + ufsUri.toString());
          }
          Arrays.sort(children, Comparator.comparing(UfsStatus::getName));

          for (UfsStatus childStatus : children) {
            if (PathUtils.isTemporaryFileName(childStatus.getName())) {
              continue;
            }
            AlluxioURI childURI = new AlluxioURI(
                PathUtils.concatPath(inodePath.getUri(), childStatus.getName()));
            if (mInodeTree.inodePathExists(childURI) && (childStatus.isFile()
                || context.getOptions().getLoadDescendantType() != LoadDescendantPType.ALL)) {
              // stop traversing if this is an existing file, or an existing directory without
              // loading all descendants.
              continue;
            }

            try (LockedInodePath descendant = inodePath.lockDescendant(
                inodePath.getUri().joinUnsafe(childStatus.getName()), LockPattern.READ)) {
              LoadMetadataContext loadMetadataContext = LoadMetadataContext
                  .mergeFrom(LoadMetadataPOptions.newBuilder()
                      .setLoadDescendantType(LoadDescendantPType.NONE).setCreateAncestors(false))
                  .setUfsStatus(childStatus);
              try {
                loadMetadataInternal(rpcContext, descendant, loadMetadataContext);
              } catch (FileNotFoundException e) {
                LOG.debug(
                    "Failed to loadMetadata because file is not in ufs:"
                        + " inodePath={}, options={}.",
                    descendant.getUri(), loadMetadataContext, e);
                continue;
              } catch (Exception e) {
                LOG.info("Failed to loadMetadata: inodePath={}, options={}.", descendant.getUri(),
                    loadMetadataContext, e);
                continue;
              }
              if (context.getOptions().getLoadDescendantType() == LoadDescendantPType.ALL
                  && descendant.getInode().isDirectory()) {
                mInodeTree.setDirectChildrenLoaded(rpcContext, descendant.getInode().asDirectory());
              }
            }
          }
          mInodeTree.setDirectChildrenLoaded(rpcContext, inodePath.getInode().asDirectory());
        }
      }
    } catch (IOException e) {
      LOG.debug("Failed to loadMetadata: inodePath={}, context={}.", inodePath.getUri(),
              context, e);
      throw e;
    }
  }

  /**
   * Loads metadata for the file identified by the given path from UFS into Alluxio.
   *
   * This method doesn't require any specific type of locking on inodePath. If the path needs to be
   * loaded, we will acquire a write-edge lock.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param resolution the UFS resolution of path
   * @param context the load metadata context
   */
  private void loadFileMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataContext context)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      FileAlreadyCompletedException, InvalidFileSizeException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    AlluxioURI ufsUri = resolution.getUri();
    long ufsBlockSizeByte;
    long ufsLength;
    AccessControlList acl = null;
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      ufsBlockSizeByte = ufs.getBlockSizeByte(ufsUri.toString());
      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingFileStatus(ufsUri.toString()));
      }
      ufsLength = ((UfsFileStatus) context.getUfsStatus()).getContentLength();

      if (isAclEnabled()) {
        Pair<AccessControlList, DefaultAccessControlList> aclPair
            = ufs.getAclPair(ufsUri.toString());
        if (aclPair != null) {
          acl = aclPair.getFirst();
          // DefaultACL should be null, because it is a file
          if (aclPair.getSecond() != null) {
            LOG.warn("File {} has default ACL in the UFS", inodePath.getUri());
          }
        }
      }
    }

    // Metadata loaded from UFS has no TTL set.
    CreateFileContext createFileContext = CreateFileContext.defaults();
    createFileContext.getOptions().setBlockSizeBytes(ufsBlockSizeByte);
    createFileContext.getOptions().setRecursive(context.getOptions().getCreateAncestors());
    createFileContext.getOptions()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createFileContext.setMetadataLoad(true);
    createFileContext.setOwner(context.getUfsStatus().getOwner());
    createFileContext.setGroup(context.getUfsStatus().getGroup());
    createFileContext.setPersisted(true);
    short ufsMode = context.getUfsStatus().getMode();
    Mode mode = new Mode(ufsMode);
    Long ufsLastModified = context.getUfsStatus().getLastModifiedTime();
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileContext.getOptions().setMode(mode.toProto());
    if (acl != null) {
      createFileContext.setAcl(acl.getEntries());
    }
    if (ufsLastModified != null) {
      createFileContext.setOperationTimeMs(ufsLastModified);
    }

    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite()) {
      createFileInternal(rpcContext, writeLockedPath, createFileContext);
      CompleteFileContext completeContext =
          CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(ufsLength))
              .setUfsStatus(context.getUfsStatus());
      if (ufsLastModified != null) {
        completeContext.setOperationTimeMs(ufsLastModified);
      }
      completeFileInternal(rpcContext, writeLockedPath, completeContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the file before we got the write lock.
      // The file already exists, so nothing needs to be loaded.
      LOG.debug("Failed to load file metadata: {}", e.toString());
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   * It is a no-op if the directory exists.
   *
   * This method doesn't require any specific type of locking on inodePath. If the path needs to be
   * loaded, we will acquire a write-edge lock if necessary.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param context the load metadata context
   */
  private void loadDirectoryMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
    createDirectoryContext.getOptions()
        .setRecursive(context.getOptions().getCreateAncestors()).setAllowExists(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createDirectoryContext.setMountPoint(mMountTable.isMountPoint(inodePath.getUri()));
    createDirectoryContext.setMetadataLoad(true);
    createDirectoryContext.setPersisted(true);
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());

    AlluxioURI ufsUri = resolution.getUri();
    AccessControlList acl = null;
    DefaultAccessControlList defaultAcl = null;
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingDirectoryStatus(ufsUri.toString()));
      }
      Pair<AccessControlList, DefaultAccessControlList> aclPair =
          ufs.getAclPair(ufsUri.toString());
      if (aclPair != null) {
        acl = aclPair.getFirst();
        defaultAcl = aclPair.getSecond();
      }
    }
    String ufsOwner = context.getUfsStatus().getOwner();
    String ufsGroup = context.getUfsStatus().getGroup();
    short ufsMode = context.getUfsStatus().getMode();
    Long lastModifiedTime = context.getUfsStatus().getLastModifiedTime();
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryContext.getOptions().setMode(mode.toProto());
    createDirectoryContext.setOwner(ufsOwner).setGroup(ufsGroup)
        .setUfsStatus(context.getUfsStatus());
    if (acl != null) {
      createDirectoryContext.setAcl(acl.getEntries());
    }

    if (defaultAcl != null) {
      createDirectoryContext.setDefaultAcl(defaultAcl.getEntries());
    }
    if (lastModifiedTime != null) {
      createDirectoryContext.setOperationTimeMs(lastModifiedTime);
    }

    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite()) {
      createDirectoryInternal(rpcContext, writeLockedPath, createDirectoryContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the directory before we got the write lock.
      // The directory already exists, so nothing needs to be loaded.
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  /**
   * Loads metadata for the path if it is (non-existing || load direct children is set).
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to load the metadata for
   * @param context the load metadata context
   */
  private void loadMetadataIfNotExist(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context) {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.READ);
    boolean inodeExists = inodePath.fullPathExists();
    boolean loadDirectChildren = false;
    if (inodeExists) {
      try {
        Inode inode = inodePath.getInode();
        loadDirectChildren = inode.isDirectory()
            && (context.getOptions().getLoadDescendantType() != LoadDescendantPType.NONE);
      } catch (FileDoesNotExistException e) {
        // This should never happen.
        throw new RuntimeException(e);
      }
    }
    if (!inodeExists || loadDirectChildren) {
      try {
        loadMetadataInternal(rpcContext, inodePath, context);
      } catch (AlluxioException | IOException e) {
        LOG.debug("Failed to load metadata for path from UFS: {}", inodePath.getUri(), e);
      }
    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.MOUNT_OPS.inc();
    LockingScheme lockingScheme = createLockingScheme(alluxioPath,
        context.getOptions().getCommonOptions(), LockPattern.WRITE_EDGE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("mount", alluxioPath, null, inodePath.getParentInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      mMountTable.checkUnderWritableMountPoint(alluxioPath);
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme, DescendantType.ONE);

      mountInternal(rpcContext, inodePath, ufsPath, context);
      auditContext.setSucceeded(true);
      Metrics.PATHS_MOUNTED.inc();
    }
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   *
   * @param rpcContext the rpc context
   * @param inodePath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param context the mount context
   */
  private void mountInternal(RpcContext rpcContext, LockedInodePath inodePath, AlluxioURI ufsPath,
      MountContext context) throws InvalidPathException, FileAlreadyExistsException,
      FileDoesNotExistException, IOException, AccessControlException {
    // Check that the Alluxio Path does not exist
    if (inodePath.fullPathExists()) {
      // TODO(calvin): Add a test to validate this (ALLUXIO-1831)
      throw new InvalidPathException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(inodePath.getUri()));
    }
    long mountId = IdUtils.createMountId();
    mountInternal(rpcContext, inodePath, ufsPath, mountId, context);
    boolean loadMetadataSucceeded = false;
    try {
      // This will create the directory at alluxioPath
      loadDirectoryMetadata(rpcContext, inodePath, LoadMetadataContext
          .mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(false)));
      loadMetadataSucceeded = true;
    } finally {
      if (!loadMetadataSucceeded) {
        mMountTable.delete(rpcContext, inodePath.getUri());
      }
    }
  }

  /**
   * Updates the mount table with the specified mount point. The mount options may be updated during
   * this method.
   *
   * @param journalContext the journal context
   * @param inodePath the Alluxio mount point
   * @param ufsPath the UFS endpoint to mount
   * @param mountId the mount id
   * @param context the mount context (may be updated)
   */
  private void mountInternal(Supplier<JournalContext> journalContext, LockedInodePath inodePath,
      AlluxioURI ufsPath, long mountId, MountContext context)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioPath = inodePath.getUri();
    // Adding the mount point will not create the UFS instance and thus not connect to UFS
    mUfsManager.addMount(mountId, new AlluxioURI(ufsPath.toString()),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global())
            .setReadOnly(context.getOptions().getReadOnly())
            .setShared(context.getOptions().getShared())
            .createMountSpecificConf(context.getOptions().getPropertiesMap()));
    try {
      try (CloseableResource<UnderFileSystem> ufsResource =
          mUfsManager.get(mountId).acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        // Check that the ufsPath exists and is a directory
        if (!ufs.isDirectory(ufsPath.toString())) {
          throw new IOException(
              ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
        }
      }
      // Check that the alluxioPath we're creating doesn't shadow a path in the parent UFS
      MountTable.Resolution resolution = mMountTable.resolve(alluxioPath);
      try (CloseableResource<UnderFileSystem> ufsResource =
               resolution.acquireUfsResource()) {
        String ufsResolvedPath = resolution.getUri().getPath();
        if (ufsResource.get().exists(ufsResolvedPath)) {
          throw new IOException(
              ExceptionMessage.MOUNT_PATH_SHADOWS_PARENT_UFS.getMessage(alluxioPath,
                  ufsResolvedPath));
        }
      }
      // Add the mount point. This will only succeed if we are not mounting a prefix of an existing
      // mount.
      mMountTable.add(journalContext, alluxioPath, ufsPath, mountId, context.getOptions().build());
    } catch (Exception e) {
      mUfsManager.removeMount(mountId);
      throw e;
    }
  }

  @Override
  public void unmount(AlluxioURI alluxioPath) throws FileDoesNotExistException,
      InvalidPathException, IOException, AccessControlException {
    Metrics.UNMOUNT_OPS.inc();
    // Unmount should lock the parent to remove the child inode.
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
            .lockInodePath(alluxioPath, LockPattern.WRITE_EDGE);
         FileSystemMasterAuditContext auditContext =
             createAuditContext("unmount", alluxioPath, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      unmountInternal(rpcContext, inodePath);
      auditContext.setSucceeded(true);
      Metrics.PATHS_UNMOUNTED.inc();
    }
  }

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   *
   * This method does not delete blocks. Instead, it adds the to the passed-in block deletion
   * context so that the blocks can be deleted after the inode deletion journal entry has been
   * written. We cannot delete blocks earlier because the inode deletion may fail, leaving us with
   * inode containing deleted blocks.
   *
   * @param rpcContext the rpc context
   * @param inodePath the Alluxio path to unmount, must be a mount point
   */
  private void unmountInternal(RpcContext rpcContext, LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException, IOException {
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    mSyncManager.stopSyncForMount(resolution.getMountId());

    if (!inodePath.fullPathExists()) {
      throw new FileDoesNotExistException(
          "Failed to unmount: Path " + inodePath.getUri() + " does not exist");
    }

    if (!mMountTable.delete(rpcContext, inodePath.getUri())) {
      throw new InvalidPathException("Failed to unmount " + inodePath.getUri() + ". Please ensure"
          + " the path is an existing mount point and not root.");
    }
    try {
      // Use the internal delete API, setting {@code alluxioOnly} to true to prevent the delete
      // operations from being persisted in the UFS.
      deleteInternal(rpcContext, inodePath, DeleteContext
          .mergeFrom(DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(true)));
    } catch (DirectoryNotEmptyException e) {
      throw new RuntimeException(String.format(
          "We should never see this exception because %s should never be thrown when recursive "
              + "is true.",
          e.getClass()));
    }
  }

  @Override
  public void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries,
      SetAclContext context)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    Metrics.SET_ACL_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, context.getOptions().getCommonOptions(), LockPattern.WRITE_INODE);
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath =
            mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
        FileSystemMasterAuditContext auditContext =
            createAuditContext("setAcl", path, null, inodePath.getInodeOrNull())) {
      mPermissionChecker.checkSetAttributePermission(inodePath, false, true);
      if (context.getOptions().getRecursive()) {
        try {
          for (LockedInodePath child : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
            mPermissionChecker.checkSetAttributePermission(child, false, true);
          }
        } catch (AccessControlException e) {
          auditContext.setAllowed(false);
          throw e;
        }
      }
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme,
          context.getOptions().getRecursive() ? DescendantType.ALL : DescendantType.NONE);
      if (!inodePath.fullPathExists()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }
      setAclInternal(rpcContext, action, inodePath, entries, context);
      auditContext.setSucceeded(true);
    }
  }

  private void setAclInternal(RpcContext rpcContext, SetAclAction action, LockedInodePath inodePath,
      List<AclEntry> entries, SetAclContext context)
      throws IOException, FileDoesNotExistException {
    Preconditions.checkState(inodePath.getLockPattern().isWrite());

    long opTimeMs = System.currentTimeMillis();
    // Check inputs for setAcl
    switch (action) {
      case REPLACE:
        Set<AclEntryType> types =
            entries.stream().map(AclEntry::getType).collect(Collectors.toSet());
        Set<AclEntryType> requiredTypes =
            Sets.newHashSet(AclEntryType.OWNING_USER, AclEntryType.OWNING_GROUP,
                AclEntryType.OTHER);
        requiredTypes.removeAll(types);

        // make sure the required entries are present
        if (!requiredTypes.isEmpty()) {
          throw new IOException(ExceptionMessage.ACL_BASE_REQUIRED.getMessage(
              String.join(", ", requiredTypes.stream().map(AclEntryType::toString).collect(
                  Collectors.toList()))));
        }
        break;
      case MODIFY: // fall through
      case REMOVE:
        if (entries.isEmpty()) {
          // Nothing to do.
          return;
        }
        break;
      case REMOVE_ALL:
        break;
      case REMOVE_DEFAULT:
        break;
      default:
    }
    setAclRecursive(rpcContext, action, inodePath, entries, false, opTimeMs, context);
  }

  private void setUfsAcl(LockedInodePath inodePath)
      throws InvalidPathException, AccessControlException {
    Inode inode = inodePath.getInodeOrNull();

    checkUfsMode(inodePath.getUri(), OperationType.WRITE);
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    String ufsUri = resolution.getUri().toString();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (ufs.isObjectStorage()) {
        LOG.warn("SetACL is not supported to object storage UFS via Alluxio. "
            + "UFS: " + ufsUri + ". This has no effect on the underlying object.");
      } else {
        try {
          List<AclEntry> entries = new ArrayList<>(inode.getACL().getEntries());
          if (inode.isDirectory()) {
            entries.addAll(inode.asDirectory().getDefaultACL().getEntries());
          }
          ufs.setAclEntries(ufsUri, entries);
        } catch (IOException e) {
          throw new AccessControlException("Could not setAcl for UFS file: " + ufsUri);
        }
      }
    }
  }

  private void setAclSingleInode(RpcContext rpcContext, SetAclAction action,
      LockedInodePath inodePath, List<AclEntry> entries, boolean replay, long opTimeMs)
      throws IOException, FileDoesNotExistException {
    Preconditions.checkState(inodePath.getLockPattern().isWrite());

    Inode inode = inodePath.getInode();

    // Check that we are not removing an extended mask.
    if (action == SetAclAction.REMOVE) {
      for (AclEntry entry : entries) {
        if ((entry.isDefault() && inode.getDefaultACL().hasExtended())
            || (!entry.isDefault() && inode.getACL().hasExtended())) {
          if (entry.getType() == AclEntryType.MASK) {
            throw new InvalidArgumentException(
                "Deleting the mask for an extended ACL is not allowed. entry: " + entry);
          }
        }
      }
    }

    mInodeTree.setAcl(rpcContext, SetAclEntry.newBuilder()
        .setId(inode.getId())
        .setOpTimeMs(opTimeMs)
        .setAction(ProtoUtils.toProto(action))
        .addAllEntries(entries.stream().map(ProtoUtils::toProto).collect(Collectors.toList()))
        .build());

    try {
      if (!replay && inode.isPersisted()) {
        setUfsAcl(inodePath);
      }
    } catch (InvalidPathException | AccessControlException e) {
      LOG.warn("Setting ufs ACL failed for path: {}", inodePath.getUri(), e);
      // TODO(david): revert the acl and default acl to the initial state if writing to ufs failed.
    }
  }

  private void setAclRecursive(RpcContext rpcContext, SetAclAction action,
      LockedInodePath inodePath, List<AclEntry> entries, boolean replay, long opTimeMs,
      SetAclContext context) throws IOException, FileDoesNotExistException {
    Preconditions.checkState(inodePath.getLockPattern().isWrite());
    setAclSingleInode(rpcContext, action, inodePath, entries, replay, opTimeMs);
    if (context.getOptions().getRecursive()) {
      for (LockedInodePath childPath : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
        setAclSingleInode(rpcContext, action, childPath, entries, replay, opTimeMs);
      }
    }
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeContext context)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException, IOException {
    SetAttributePOptions.Builder options = context.getOptions();
    Metrics.SET_ATTRIBUTE_OPS.inc();
    // for chown
    boolean rootRequired = options.hasOwner();
    // for chgrp, chmod
    boolean ownerRequired = (options.hasGroup()) || (options.hasMode());
    if (options.hasOwner() && options.hasGroup()) {
      try {
        checkUserBelongsToGroup(options.getOwner(), options.getGroup());
      } catch (IOException e) {
        throw new IOException(String.format("Could not update owner:group for %s to %s:%s. %s",
            path.toString(), options.getOwner(), options.getGroup(), e.toString()), e);
      }
    }
    String commandName;
    if (options.hasOwner()) {
      commandName = "chown";
    } else if (options.hasGroup()) {
      commandName = "chgrp";
    } else if (options.hasMode()) {
      commandName = "chmod";
    } else {
      commandName = "setAttribute";
    }
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), LockPattern.WRITE_INODE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext(commandName, path, null, inodePath.getInodeOrNull())) {
      mMountTable.checkUnderWritableMountPoint(path);
      // Force recursive sync metadata if it is a pinning and unpinning operation
      boolean recursiveSync = options.hasPinned() || options.getRecursive();

      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme,
          recursiveSync ? DescendantType.ALL : DescendantType.ONE);
      if (!inodePath.fullPathExists()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }
      try {
        mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
        if (context.getOptions().getRecursive()) {
          for (LockedInodePath childPath : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
            mPermissionChecker.checkSetAttributePermission(childPath, rootRequired, ownerRequired);
          }
        }
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }

      setAttributeInternal(rpcContext, inodePath, context);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Checks whether the owner belongs to the group.
   *
   * @param owner the owner to check
   * @param group the group to check
   * @throws FailedPreconditionException if owner does not belong to group
   */
  private void checkUserBelongsToGroup(String owner, String group)
      throws IOException {
    List<String> groups = CommonUtils.getGroups(owner, ServerConfiguration.global());
    if (groups == null || !groups.contains(group)) {
      throw new FailedPreconditionException("Owner " + owner
          + " does not belong to the group " + group);
    }
  }

  /**
   * Sets the file attribute.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to set attribute for
   * @param context attributes to be set, see {@link SetAttributePOptions}
   */
  private void setAttributeInternal(RpcContext rpcContext, LockedInodePath inodePath,
      SetAttributeContext context)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException, IOException {
    Inode targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (context.getOptions().getRecursive() && targetInode.isDirectory()) {
      for (LockedInodePath childPath : mInodeTree.getImplicitlyLockedDescendants(inodePath)) {
        setAttributeSingleFile(rpcContext, childPath, true, opTimeMs, context);
      }
    }
    setAttributeSingleFile(rpcContext, inodePath, true, opTimeMs, context);
  }

  @Override
  public void scheduleAsyncPersistence(AlluxioURI path)
      throws AlluxioException, UnavailableException {
    // We retry an async persist request until ufs permits the operation
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, LockPattern.WRITE_INODE)) {
      if (!inodePath.getInodeFile().isCompleted()) {
        throw new InvalidPathException(
            "Cannot persist an incomplete Alluxio file: " + inodePath.getUri());
      }
      mInodeTree.updateInode(rpcContext, UpdateInodeEntry.newBuilder()
          .setId(inodePath.getInode().getId())
          .setPersistenceState(PersistenceState.TO_BE_PERSISTED.name())
          .build());
      mPersistRequests.put(inodePath.getInode().getId(), new alluxio.time.ExponentialTimer(
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS)));
    }
  }

  /**
   * Actively sync metadata, based on a list of changed files.
   *
   * @param path the path to sync
   * @param changedFiles collection of files that are changed under the path to sync, if this is
   *        null, force sync the entire directory
   * @param executorService executor to execute the parallel incremental sync
   */
  public void activeSyncMetadata(AlluxioURI path, Collection<AlluxioURI> changedFiles,
      ExecutorService executorService) throws IOException {
    if (changedFiles == null) {
      LOG.info("Start an active full sync of {}", path.toString());
    } else {
      LOG.info("Start an active incremental sync of {} files", changedFiles.size());
    }

    if (changedFiles != null && changedFiles.isEmpty()) {
      return;
    }

    Map<AlluxioURI, UfsStatus> statusCache;
    try (RpcContext rpcContext = createRpcContext()) {
      statusCache = populateStatusCache(path, DescendantType.ALL);
      if (changedFiles == null) {
        LockingScheme lockingScheme = new LockingScheme(path, LockPattern.READ, true);
        try (LockedInodePath inodePath =
            mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern())) {
          syncMetadataInternal(rpcContext, inodePath, lockingScheme, DescendantType.ALL,
              statusCache);
        }
        LOG.info("Ended an active full sync of {}", path.toString());
        return;
      } else {
        Set<Callable<Void>> callables = new HashSet<>();
        for (AlluxioURI changedFile : changedFiles) {
          callables.add(() -> {
            LockingScheme lockingScheme = new LockingScheme(path, LockPattern.READ, true);
            try (LockedInodePath changedFilePath =
                mInodeTree.lockInodePath(changedFile, lockingScheme.getPattern())) {
              syncMetadataInternal(rpcContext, changedFilePath, lockingScheme, DescendantType.NONE,
                  statusCache);
            } catch (InvalidPathException e) {
              LOG.info("forceSyncMetadata processed an invalid path {}", changedFile.getPath());
            }
            return null;
          });
        }
        executorService.invokeAll(callables);
      }
    } catch (InvalidPathException e) {
      LOG.warn("InvalidPathException during active sync {}", e);
    } catch (InterruptedException e) {
      LOG.warn("InterruptedException during active sync {}", e);
      Thread.currentThread().interrupt();
      return;
    }
    LOG.info("Ended an active incremental sync of {} files", changedFiles.size());
  }

  @Override
  public boolean recordActiveSyncTxid(long txId, long mountId) {
    MountInfo mountInfo = mMountTable.getMountInfo(mountId);
    if (mountInfo == null) {
      return false;
    }
    AlluxioURI mountPath = mountInfo.getAlluxioUri();

    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockFullInodePath(mountPath, LockPattern.READ)) {
      File.ActiveSyncTxIdEntry txIdEntry =
          File.ActiveSyncTxIdEntry.newBuilder().setTxId(txId).setMountId(mountId).build();
      rpcContext.journal(JournalEntry.newBuilder().setActiveSyncTxId(txIdEntry).build());
    } catch (UnavailableException | InvalidPathException | FileDoesNotExistException e) {
      LOG.warn("Exception when recording activesync txid, path {}, exception {}",
          mountPath, e);
      return false;
    }

    return true;
  }

  private boolean syncMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LockingScheme lockingScheme, DescendantType syncDescendantType) {
    boolean result;
    if (!lockingScheme.shouldSync()) {
      return false;
    }
    try {
      result = syncMetadataInternal(rpcContext, inodePath, lockingScheme,
          syncDescendantType, populateStatusCache(inodePath.getUri(), syncDescendantType));
    } catch (Exception e) {
      LOG.warn("Sync metadata for path {} encountered exception {}", inodePath.getUri(),
          Throwables.getStackTraceAsString(e));
      return false;
    }
    return result;
  }

  private Map<AlluxioURI, UfsStatus> populateStatusCache(AlluxioURI path,
      DescendantType syncDescendantType) {
    Map<AlluxioURI, UfsStatus> statusCache = new HashMap<>();
    try {
      MountTable.Resolution resolution = mMountTable.resolve(path);
      AlluxioURI ufsUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        ListOptions listOptions = ListOptions.defaults();
        // statusCache stores uri to ufsstatus mapping that is used to construct fingerprint

        listOptions.setRecursive(syncDescendantType == DescendantType.ALL);
        try {
          UfsStatus[] children = ufs.listStatus(ufsUri.toString(), listOptions);
          if (children != null) {
            for (UfsStatus childStatus : children) {
              statusCache.put(path.joinUnsafe(childStatus.getName()),
                  childStatus);
            }
          }
        } catch (Exception e) {
          LOG.debug("ListStatus failed as an preparation step for syncMetadata {}", path, e);
        }
        return statusCache;
      }
    } catch (InvalidPathException e) {
      return statusCache;
    }
  }

  /**
   * Syncs the Alluxio metadata with UFS.
   *
   * This method expects that the last existing edge leading to inodePath has been write-locked.
   *
   * @param rpcContext the rpcContext
   * @param inodePath the Alluxio inode path to sync with UFS
   * @param lockingScheme the locking scheme used to lock the inode path
   * @param syncDescendantType how to sync descendants
   * @param statusCache a cache provided to the sync method which stores the UfsStatus of files
   * @return true if the sync was performed successfully, false otherwise (including errors)
   */
  private boolean syncMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      LockingScheme lockingScheme, DescendantType syncDescendantType,
      Map<AlluxioURI, UfsStatus> statusCache)
      throws IOException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    // The high-level process for the syncing is:
    // 1. Find all Alluxio paths which are not consistent with the corresponding UFS path.
    //    This means the UFS path does not exist, or is different from the Alluxio metadata.
    // 2. If only the metadata changed for a file or a directory, update the inode with
    //    new metadata from the UFS.
    // 3. Delete any Alluxio path whose content is not consistent with UFS, or not in UFS. After
    //    this step, all the paths in Alluxio are consistent with UFS, and there may be additional
    //    UFS paths to load.
    // 4. Load metadata from UFS.

    Set<String> pathsToLoad = new HashSet<>();

    try {
      if (!inodePath.fullPathExists()) {
        // The requested path does not exist in Alluxio, so just load metadata.
        pathsToLoad.add(inodePath.getUri().getPath());
      } else {
        SyncResult result =
            syncInodeMetadata(rpcContext, inodePath, syncDescendantType, statusCache);
        if (result.getDeletedInode()) {
          // If the inode was deleted, then the inode path should reflect the delete.
          inodePath.removeLastInode();
        }
        pathsToLoad = result.getPathsToLoad();
      }
    } catch (InvalidPathException | FileDoesNotExistException | AccessControlException e) {
      LOG.warn("Exception encountered when syncing metadata for {}, exception is {}",
          inodePath.getUri(), e);
      return false;
    } finally {
      inodePath.downgradeToPattern(lockingScheme.getDesiredPattern());
    }

    // Update metadata for all the mount points
    for (String mountPoint : pathsToLoad) {
      AlluxioURI mountPointUri = new AlluxioURI(mountPoint);
      try {
        if (PathUtils.hasPrefix(inodePath.getUri().getPath(), mountPointUri.getPath())) {
          // one of the mountpoint is above the original inodePath, we start loading from the
          // original inodePath. It is already locked. so we proceed to load metadata.
          try {
            loadMetadataInternal(rpcContext, inodePath,
                LoadMetadataContext
                    .mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(true)
                        .setLoadDescendantType(GrpcUtils.toProto(syncDescendantType))));

            mUfsSyncPathCache.notifySyncedPath(inodePath.getUri().getPath());
          } catch (Exception e) {
            // This may be expected. For example, when creating a new file, the UFS file is not
            // expected to exist.
            LOG.debug("Failed to load metadata for path: {}", inodePath.getUri(), e);
            continue;
          }
        } else {
          try (LockedInodePath descendantPath =
              inodePath.lockDescendant(mountPointUri, LockPattern.READ)) {
            try {
              loadMetadataInternal(rpcContext, descendantPath,
                  LoadMetadataContext
                      .mergeFrom(LoadMetadataPOptions.newBuilder().setCreateAncestors(true)
                          .setLoadDescendantType(GrpcUtils.toProto(syncDescendantType))));
            } catch (Exception e) {
              LOG.debug("Failed to load metadata for mount point: {}", mountPointUri, e);
            }
            mUfsSyncPathCache.notifySyncedPath(mountPoint);
          }
        }
      } catch (InvalidPathException e) {
        LOG.warn("Tried to update metadata from an invalid path : {}", mountPointUri.getPath(), e);
      }
    }
    try {
      // Re-traverse to pick up newly created inodes on the path.
      inodePath.traverse();
    } catch (InvalidPathException e) {
      throw new RuntimeException(e);
    }

    if (pathsToLoad.isEmpty()) {
      mUfsSyncPathCache.notifySyncedPath(inodePath.getUri().getPath());
    }
    return true;
  }

  @VisibleForTesting
  ReadOnlyInodeStore getInodeStore() {
    return mInodeStore;
  }

  /**
   * This class represents the result for a sync. The following are returned:
   * - deleted: if true, the inode was already deleted as part of the syncing process
   * - pathsToLoad: a set of paths that need to be loaded from UFS.
   */
  private static class SyncResult {
    private boolean mDeletedInode;
    private Set<String> mPathsToLoad;

    static SyncResult defaults() {
      return new SyncResult(false, new HashSet<>());
    }

    SyncResult(boolean deletedInode, Set<String> pathsToLoad) {
      mDeletedInode = deletedInode;
      mPathsToLoad = new HashSet<>(pathsToLoad);
    }

    boolean getDeletedInode() {
      return mDeletedInode;
    }

    Set<String> getPathsToLoad() {
      return mPathsToLoad;
    }
  }

  /**
   * Syncs an inode with the UFS.
   *
   * @param rpcContext the rpc context
   * @param inodePath the Alluxio inode path to sync with UFS
   * @param syncDescendantType how to sync descendants
   * @param statusCache a pre-populated cache of ufs statuses that can be used to construct
   *                    fingerprint
   * @return the result of the sync, including if the inode was deleted, and if further load
   *         metadata is required
   */
  private SyncResult syncInodeMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      DescendantType syncDescendantType, Map<AlluxioURI, UfsStatus> statusCache)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    Preconditions.checkState(inodePath.getLockPattern() == LockPattern.WRITE_EDGE);

    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // Set of paths to sync
    Set<String> pathsToLoad = new HashSet<>();
    LOG.debug("Syncing inode metadata {}", inodePath.getUri());
    // The options for deleting.
    DeleteContext syncDeleteContext = DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(true).setUnchecked(true));

    // The requested path already exists in Alluxio.
    Inode inode = inodePath.getInode();

    if (inode instanceof InodeFile && !inode.asFile().isCompleted()) {
      // Do not sync an incomplete file, since the UFS file is expected to not exist.
      return SyncResult.defaults();
    }
    Optional<Scoped> persistingLock = mInodeLockManager.tryAcquirePersistingLock(inode.getId());
    if (!persistingLock.isPresent()) {
      // Do not sync a file in the process of being persisted, since the UFS file is being
      // written.
      return SyncResult.defaults();
    }
    persistingLock.get().close();

    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      String ufsFingerprint;
      Fingerprint ufsFpParsed;
      UfsStatus cachedStatus = statusCache.get(inodePath.getUri());
      if (cachedStatus == null) {
        // TODO(david): change the interface so that getFingerprint returns a parsed fingerprint
        ufsFingerprint = ufs.getFingerprint(ufsUri.toString());
        ufsFpParsed = Fingerprint.parse(ufsFingerprint);
      } else {
        Pair<AccessControlList, DefaultAccessControlList> aclPair
            = ufs.getAclPair(ufsUri.toString());

        if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
          ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus);
          ufsFingerprint = ufsFpParsed.serialize();
        } else {
          ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus,
              aclPair.getFirst());
          ufsFingerprint = ufsFpParsed.serialize();
        }
      }

      boolean containsMountPoint = mMountTable.containsMountPoint(inodePath.getUri());

      UfsSyncUtils.SyncPlan syncPlan =
          UfsSyncUtils.computeSyncPlan(inode, ufsFpParsed, containsMountPoint);

      if (syncPlan.toUpdateMetaData()) {
        // UpdateMetadata is used when a file or a directory only had metadata change.
        // It works by calling SetAttributeInternal on the inodePath.
        if (ufsFpParsed.isValid()) {
          short mode = Short.parseShort(ufsFpParsed.getTag(Tag.MODE));
          long opTimeMs = System.currentTimeMillis();
          setAttributeSingleFile(rpcContext, inodePath, false, opTimeMs, SetAttributeContext
              .mergeFrom(SetAttributePOptions.newBuilder().setOwner(ufsFpParsed.getTag(Tag.OWNER))
                  .setGroup(ufsFpParsed.getTag(Tag.GROUP)).setMode(new Mode(mode).toProto()))
              .setUfsFingerprint(ufsFingerprint));
        }
      }
      if (syncPlan.toDelete()) {
        try {
          deleteInternal(rpcContext, inodePath, syncDeleteContext);

          deletedInode = true;
        } catch (DirectoryNotEmptyException | IOException e) {
          // Should not happen, since it is an unchecked delete.
          LOG.error("Unexpected error for unchecked delete.", e);
        }
      }
      if (syncPlan.toLoadMetadata()) {
        AlluxioURI mountUri = new AlluxioURI(mMountTable.getMountPoint(inodePath.getUri()));
        pathsToLoad.add(mountUri.getPath());
      }
      if (syncPlan.toSyncChildren() && inode.isDirectory()
          && syncDescendantType != DescendantType.NONE) {
        // maps children name to inode
        Map<String, Inode> inodeChildren = new HashMap<>();
        for (Inode child : mInodeStore.getChildren(inode.asDirectory())) {
          inodeChildren.put(child.getName(), child);
        }

        UfsStatus[] listStatus = ufs.listStatus(ufsUri.toString());
        // Iterate over UFS listings and process UFS children.
        if (listStatus != null) {
          for (UfsStatus ufsChildStatus : listStatus) {
            if (!inodeChildren.containsKey(ufsChildStatus.getName()) && !PathUtils
                .isTemporaryFileName(ufsChildStatus.getName())) {
              // Ufs child exists, but Alluxio child does not. Must load metadata.
              AlluxioURI mountUri = new AlluxioURI(mMountTable.getMountPoint(inodePath.getUri()));
              pathsToLoad.add(mountUri.getPath());
              break;
            }
          }
        }

        // Iterate over Alluxio children and process persisted children.
        for (Map.Entry<String, Inode> inodeEntry : inodeChildren.entrySet()) {
          if (!inodeEntry.getValue().isPersisted()) {
            // Ignore non-persisted inodes.
            continue;
          }

          // Technically we don't need to lock here since inodePath is already write-locked. We can
          // improve this by implementing a way to traverse an inode path without locking.
          try (LockedInodePath descendant = inodePath.lockDescendant(
              inodePath.getUri().joinUnsafe(inodeEntry.getKey()), LockPattern.WRITE_EDGE)) {
            // Recursively sync children
            if (syncDescendantType != DescendantType.ALL) {
              syncDescendantType = DescendantType.NONE;
            }
            SyncResult syncResult =
                syncInodeMetadata(rpcContext, descendant, syncDescendantType, statusCache);
            pathsToLoad.addAll(syncResult.getPathsToLoad());
          }
        }
      }
    }
    return new SyncResult(deletedInode, pathsToLoad);
  }

  @Override
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles,
      WorkerHeartbeatContext context) throws IOException {

    List<String> persistedUfsFingerprints = context.getOptions().getPersistedFileFingerprintsList();
    boolean hasPersistedFingerprints = persistedUfsFingerprints.size() == persistedFiles.size();
    for (int i = 0; i < persistedFiles.size(); i++) {
      long fileId = persistedFiles.get(i);
      String ufsFingerprint = hasPersistedFingerprints ? persistedUfsFingerprints.get(i) :
          Constants.INVALID_UFS_FINGERPRINT;
      try {
        // Permission checking for each file is performed inside setAttribute
        setAttribute(getPath(fileId),
            SetAttributeContext
                .mergeFrom(SetAttributePOptions.newBuilder().setPersisted(true))
                .setUfsFingerprint(ufsFingerprint));
      } catch (FileDoesNotExistException | AccessControlException | InvalidPathException e) {
        LOG.error("Failed to set file {} as persisted, because {}", fileId, e);
      }
    }

    // TODO(zac) Clean up master and worker code since this is taken care of by job service now.
    // Worker should not persist any files. Instead, files are persisted through job service.
    List<PersistFile> filesToPersist = new ArrayList<>();
    FileSystemCommandOptions commandOptions = new FileSystemCommandOptions();
    commandOptions.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, commandOptions);
  }

  /**
   * @param inodePath the {@link LockedInodePath} to use
   * @param updateUfs whether to update the UFS with the attribute change
   * @param opTimeMs the operation time (in milliseconds)
   * @param context the method context
   */
  private void setAttributeSingleFile(RpcContext rpcContext, LockedInodePath inodePath,
      boolean updateUfs, long opTimeMs, SetAttributeContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Inode inode = inodePath.getInode();
    SetAttributePOptions.Builder protoOptions = context.getOptions();
    if (protoOptions.hasPinned()) {
      mInodeTree.setPinned(rpcContext, inodePath, context.getOptions().getPinned(), opTimeMs);
    }
    UpdateInodeEntry.Builder entry = UpdateInodeEntry.newBuilder().setId(inode.getId());
    if (protoOptions.hasReplicationMax() || protoOptions.hasReplicationMin()) {
      Integer replicationMax =
          protoOptions.hasReplicationMax() ? protoOptions.getReplicationMax() : null;
      Integer replicationMin =
          protoOptions.hasReplicationMin() ? protoOptions.getReplicationMin() : null;
      mInodeTree.setReplication(rpcContext, inodePath, replicationMax, replicationMin, opTimeMs);
    }
    // protoOptions may not have both fields set
    if (protoOptions.hasCommonOptions()) {
      FileSystemMasterCommonPOptions commonOpts = protoOptions.getCommonOptions();
      TtlAction action = commonOpts.hasTtlAction() ? commonOpts.getTtlAction() : null;
      Long ttl = commonOpts.hasTtl() ? commonOpts.getTtl() : null;
      boolean modified = false;

      if (ttl != null && inode.getTtl() != ttl) {
        entry.setTtl(ttl);
        modified = true;
      }
      if (action != null && inode.getTtlAction() != action) {
        entry.setTtlAction(ProtobufUtils.toProtobuf(action));
        modified = true;
      }

      if (modified) {
        entry.setLastModificationTimeMs(opTimeMs);
      }
    }
    if (protoOptions.hasPersisted()) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(inode.asFile().isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions
          .checkArgument(protoOptions.getPersisted(), PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!inode.asFile().isPersisted()) {
        entry.setPersistenceState(PersistenceState.PERSISTED.name());
        entry.setLastModificationTimeMs(context.getOperationTimeMs());
        propagatePersistedInternal(rpcContext, inodePath);
        Metrics.FILES_PERSISTED.inc();
      }
    }
    boolean ownerGroupChanged = (protoOptions.hasOwner()) || (protoOptions.hasGroup());
    boolean modeChanged = protoOptions.hasMode();
    // If the file is persisted in UFS, also update corresponding owner/group/permission.
    if ((ownerGroupChanged || modeChanged) && updateUfs && inode.isPersisted()) {
      if ((inode instanceof InodeFile) && !inode.asFile().isCompleted()) {
        LOG.debug("Alluxio does not propagate chown/chgrp/chmod to UFS for incomplete files.");
      } else {
        checkUfsMode(inodePath.getUri(), OperationType.WRITE);
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        String ufsUri = resolution.getUri().toString();
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          if (ufs.isObjectStorage()) {
            LOG.debug("setOwner/setMode is not supported to object storage UFS via Alluxio. "
                + "UFS: " + ufsUri + ". This has no effect on the underlying object.");
          } else {
            String owner = null;
            String group = null;
            String mode = null;
            if (ownerGroupChanged) {
              try {
                owner =
                    protoOptions.getOwner() != null ? protoOptions.getOwner() : inode.getOwner();
                group =
                    protoOptions.getGroup() != null ? protoOptions.getGroup() : inode.getGroup();
                ufs.setOwner(ufsUri, owner, group);
              } catch (IOException e) {
                throw new AccessControlException("Could not setOwner for UFS file " + ufsUri
                    + " . Aborting the setAttribute operation in Alluxio.", e);
              }
            }
            if (modeChanged) {
              try {
                mode = String.valueOf(protoOptions.getMode());
                ufs.setMode(ufsUri, ModeUtils.protoToShort(protoOptions.getMode()));
              } catch (IOException e) {
                throw new AccessControlException("Could not setMode for UFS file " + ufsUri
                    + " . Aborting the setAttribute operation in Alluxio.", e);
              }
            }
            // Retrieve the ufs fingerprint after the ufs changes.
            String existingFingerprint = inode.getUfsFingerprint();
            if (!existingFingerprint.equals(Constants.INVALID_UFS_FINGERPRINT)) {
              // Update existing fingerprint, since contents did not change
              Fingerprint fp = Fingerprint.parse(existingFingerprint);
              fp.putTag(Fingerprint.Tag.OWNER, owner);
              fp.putTag(Fingerprint.Tag.GROUP, group);
              fp.putTag(Fingerprint.Tag.MODE, mode);
              context.setUfsFingerprint(fp.serialize());
            } else {
              // Need to retrieve the fingerprint from ufs.
              context.setUfsFingerprint(ufs.getFingerprint(ufsUri));
            }
          }
        }
      }
    }
    if (!context.getUfsFingerprint().equals(Constants.INVALID_UFS_FINGERPRINT)) {
      entry.setUfsFingerprint(context.getUfsFingerprint());
    }
    // Only commit the set permission to inode after the propagation to UFS succeeded.
    if (protoOptions.hasOwner()) {
      entry.setOwner(protoOptions.getOwner());
    }
    if (protoOptions.hasGroup()) {
      entry.setGroup(protoOptions.getGroup());
    }
    if (modeChanged) {
      entry.setMode(ModeUtils.protoToShort(protoOptions.getMode()));
    }
    mInodeTree.updateInode(rpcContext, entry.build());
  }

  @Override
  public List<SyncPointInfo> getSyncPathList() {
    return mSyncManager.getSyncPathList();
  }

  private void startSyncAndJournal(RpcContext rpcContext, AlluxioURI uri)
      throws InvalidPathException, IOException {
    try (LockResource r = new LockResource(mSyncManager.getSyncManagerLock())) {
      MountTable.Resolution resolution = mMountTable.resolve(uri);
      long mountId = resolution.getMountId();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        if (!ufsResource.get().supportsActiveSync()) {
          throw new UnsupportedOperationException("Active Syncing is not supported on this UFS type"
              + ufsResource.get().getUnderFSType());
        }
      }

      if (mSyncManager.isActivelySynced(uri)) {
        throw new InvalidPathException("URI " + uri + " is already a sync point");
      }
      AddSyncPointEntry addSyncPoint =
          AddSyncPointEntry.newBuilder()
              .setSyncpointPath(uri.toString())
              .setMountId(mountId)
              .build();
      mSyncManager.applyAndJournal(rpcContext, addSyncPoint);
      try {
        mSyncManager.startSyncPostJournal(uri);
      } catch (Throwable e) {
        // revert state;
        RemoveSyncPointEntry removeSyncPoint =
            File.RemoveSyncPointEntry.newBuilder()
                .setSyncpointPath(uri.toString()).build();
        mSyncManager.applyAndJournal(rpcContext, removeSyncPoint);
        mSyncManager.recoverFromStartSync(uri, resolution.getMountId());
        throw e;
      }
    }
  }

  @Override
  public void startSync(AlluxioURI syncPoint)
      throws IOException, InvalidPathException, AccessControlException, ConnectionFailedException {
    LockingScheme lockingScheme = new LockingScheme(syncPoint, LockPattern.WRITE_EDGE, true);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("startSync", syncPoint, null,
                 inodePath.getParentInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      startSyncAndJournal(rpcContext, syncPoint);
      auditContext.setSucceeded(true);
    }
  }

  private void stopSyncAndJournal(RpcContext rpcContext,
      LockingScheme lockingScheme, LockedInodePath lockedInodePath)
      throws IOException, InvalidPathException {
    try (LockResource r = new LockResource(mSyncManager.getSyncManagerLock())) {
      MountTable.Resolution resolution = mSyncManager.resolveSyncPoint(lockedInodePath.getUri());
      if (resolution == null) {
        throw new InvalidPathException(lockedInodePath.getUri() + " is not a sync point.");
      }
      AlluxioURI uri = lockedInodePath.getUri();
      RemoveSyncPointEntry removeSyncPoint = File.RemoveSyncPointEntry.newBuilder()
              .setSyncpointPath(lockedInodePath.getUri().toString())
              .setMountId(resolution.getMountId())
              .build();
      mSyncManager.applyAndJournal(rpcContext, removeSyncPoint);

      try {
        mSyncManager.stopSyncPostJournal(lockedInodePath.getUri());
      } catch (Throwable e) {
        // revert state;
        AddSyncPointEntry addSyncPoint =
            File.AddSyncPointEntry.newBuilder()
                .setSyncpointPath(uri.toString()).build();
        mSyncManager.applyAndJournal(rpcContext, addSyncPoint);
        mSyncManager.recoverFromStopSync(uri, resolution.getMountId());
        throw e;
      }
    }
  }

  @Override
  public void stopSync(AlluxioURI syncPoint)
      throws IOException, InvalidPathException, AccessControlException {
    LockingScheme lockingScheme = new LockingScheme(syncPoint, LockPattern.WRITE_EDGE, false);
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath =
            mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getPattern());
        FileSystemMasterAuditContext auditContext =
             createAuditContext("stopSync", syncPoint, null,
                 inodePath.getParentInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      stopSyncAndJournal(rpcContext, lockingScheme, inodePath);
      auditContext.setSucceeded(true);
    }
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    return mBlockMaster.getWorkerInfoList();
  }

  /**
   * @param fileId file ID
   * @param jobId persist job ID
   * @param uri Alluxio Uri of the file
   * @param tempUfsPath temp UFS path
   */
  private void addPersistJob(long fileId, long jobId, AlluxioURI uri, String tempUfsPath) {
    alluxio.time.ExponentialTimer timer = mPersistRequests.remove(fileId);
    if (timer == null) {
      timer = new alluxio.time.ExponentialTimer(
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_INTERVAL_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_INITIAL_WAIT_TIME_MS),
          ServerConfiguration.getMs(PropertyKey.MASTER_PERSISTENCE_MAX_TOTAL_WAIT_TIME_MS));
    }
    mPersistJobs.put(fileId, new PersistJob(jobId, fileId, uri, tempUfsPath, timer));
  }

  /**
   * Periodically schedules jobs to persist files and updates metadata accordingly.
   */
  @NotThreadSafe
  private final class PersistenceScheduler implements alluxio.heartbeat.HeartbeatExecutor {
    private static final long MAX_QUIET_PERIOD_SECONDS = 64;

    /**
     * Quiet period for job service flow control (in seconds). When job service refuses starting new
     * jobs, we use exponential backoff to alleviate the job service pressure.
     */
    private long mQuietPeriodSeconds;

    /**
     * Creates a new instance of {@link PersistenceScheduler}.
     */
    PersistenceScheduler() {
      mQuietPeriodSeconds = 0;
    }

    @Override
    public void close() {} // Nothing to clean up

    /**
     * Updates the file system metadata to reflect the fact that the persist file request expired.
     *
     * @param fileId the file ID
     */
    private void handleExpired(long fileId) throws AlluxioException, UnavailableException {
      try (JournalContext journalContext = createJournalContext();
           LockedInodePath inodePath = mInodeTree
               .lockFullInodePath(fileId, LockPattern.WRITE_INODE)) {
        InodeFile inode = inodePath.getInodeFile();
        switch (inode.getPersistenceState()) {
          case LOST:
            // fall through
          case NOT_PERSISTED:
            // fall through
          case PERSISTED:
            LOG.warn("File {} (id={}) persistence state is {} and will not be changed.",
                inodePath.getUri(), fileId, inode.getPersistenceState());
            return;
          case TO_BE_PERSISTED:
            mInodeTree.updateInode(journalContext, UpdateInodeEntry.newBuilder()
                .setId(inode.getId())
                .setPersistenceState(PersistenceState.NOT_PERSISTED.name())
                .build());
            mInodeTree.updateInodeFile(journalContext, UpdateInodeFileEntry.newBuilder()
                .setId(inode.getId())
                .setPersistJobId(Constants.PERSISTENCE_INVALID_JOB_ID)
                .setTempUfsPath(Constants.PERSISTENCE_INVALID_UFS_PATH)
                .build());
            break;
          default:
            throw new IllegalStateException(
                "Unrecognized persistence state: " + inode.getPersistenceState());
        }
      }
    }

    /**
     * Attempts to schedule a persist job and updates the file system metadata accordingly.
     *
     * @param fileId the file ID
     */
    private void handleReady(long fileId) throws AlluxioException, IOException {
      alluxio.time.ExponentialTimer timer = mPersistRequests.get(fileId);
      // Lookup relevant file information.
      AlluxioURI uri;
      String tempUfsPath;
      try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, LockPattern.READ)) {
        InodeFile inode = inodePath.getInodeFile();
        uri = inodePath.getUri();
        switch (inode.getPersistenceState()) {
          case LOST:
            // fall through
          case NOT_PERSISTED:
            // fall through
          case PERSISTED:
            LOG.warn("File {} (id={}) persistence state is {} and will not be changed.",
                inodePath.getUri(), fileId, inode.getPersistenceState());
            return;
          case TO_BE_PERSISTED:
            tempUfsPath = inodePath.getInodeFile().getTempUfsPath();
            break;
          default:
            throw new IllegalStateException(
                "Unrecognized persistence state: " + inode.getPersistenceState());
        }
      }

      MountTable.Resolution resolution = mMountTable.resolve(uri);
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        // If previous persist job failed, clean up the temporary file.
        cleanup(ufsResource.get(), tempUfsPath);
      }

      // Generate a temporary path to be used by the persist job.
      tempUfsPath =
          PathUtils.temporaryFileName(System.currentTimeMillis(), resolution.getUri().toString());
      alluxio.job.persist.PersistConfig config =
          new alluxio.job.persist.PersistConfig(uri.getPath(), resolution.getMountId(), false,
              tempUfsPath);

      // Schedule the persist job.
      long jobId;
      JobMasterClient client = mJobMasterClientPool.acquire();
      try {
        jobId = client.run(config);
      } finally {
        mJobMasterClientPool.release(client);
      }
      mQuietPeriodSeconds /= 2;
      mPersistJobs.put(fileId, new PersistJob(jobId, fileId, uri, tempUfsPath, timer));

      // Update the inode and journal the change.
      try (JournalContext journalContext = createJournalContext();
           LockedInodePath inodePath = mInodeTree
               .lockFullInodePath(fileId, LockPattern.WRITE_INODE)) {
        InodeFile inode = inodePath.getInodeFile();
        mInodeTree.updateInodeFile(journalContext, UpdateInodeFileEntry.newBuilder()
            .setId(inode.getId())
            .setPersistJobId(jobId)
            .setTempUfsPath(tempUfsPath)
            .build());
      }
    }

    /**
     * {@inheritDoc}
     *
     * The method iterates through the set of files to be persisted (identified by their ID) and
     * attempts to schedule a file persist job. Each iteration removes the file ID from the set
     * of files to be persisted unless the execution sets the {@code remove} flag to false.
     *
     * @throws InterruptedException if the thread is interrupted
     */
    @Override
    public void heartbeat() throws InterruptedException {
      java.util.concurrent.TimeUnit.SECONDS.sleep(mQuietPeriodSeconds);
      // Process persist requests.
      for (long fileId : mPersistRequests.keySet()) {
        boolean remove = true;
        alluxio.time.ExponentialTimer timer = mPersistRequests.get(fileId);
        if (timer == null) {
          // This could occur if a key is removed from mPersistRequests while we are iterating.
          continue;
        }
        alluxio.time.ExponentialTimer.Result timerResult = timer.tick();
        if (timerResult == alluxio.time.ExponentialTimer.Result.NOT_READY) {
          // operation is not ready to be scheduled
          continue;
        }
        AlluxioURI uri = null;
        try {
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(fileId, LockPattern.READ)) {
            uri = inodePath.getUri();
          }
          try {
            checkUfsMode(uri, OperationType.WRITE);
          } catch (Exception e) {
            LOG.warn("Unable to schedule persist request for path {}: {}", uri, e.getMessage());
            // Retry when ufs mode permits operation
            remove = false;
            continue;
          }
          switch (timerResult) {
            case EXPIRED:
              handleExpired(fileId);
              break;
            case READY:
              handleReady(fileId);
              break;
            default:
              throw new IllegalStateException("Unrecognized timer state: " + timerResult);
          }
        } catch (FileDoesNotExistException | InvalidPathException e) {
          LOG.warn("The file {} (id={}) to be persisted was not found : {}", uri, fileId,
              e.getMessage());
          LOG.debug("Exception: ", e);
        } catch (UnavailableException e) {
          LOG.warn("Failed to persist file {}, will retry later: {}", uri, e.toString());
          remove = false;
        } catch (ResourceExhaustedException e) {
          LOG.warn("The job service is busy, will retry later: {}", e.toString());
          LOG.debug("Exception: ", e);
          mQuietPeriodSeconds = (mQuietPeriodSeconds == 0) ? 1 :
              Math.min(MAX_QUIET_PERIOD_SECONDS, mQuietPeriodSeconds * 2);
          remove = false;
          // End the method here until the next heartbeat. No more jobs should be scheduled during
          // the current heartbeat if the job master is at full capacity.
          return;
        } catch (Exception e) {
          LOG.warn("Unexpected exception encountered when scheduling the persist job for file {} "
              + "(id={}) : {}", uri, fileId, e.getMessage());
          LOG.debug("Exception: ", e);
        } finally {
          if (remove) {
            mPersistRequests.remove(fileId);
          }
        }
      }
    }
  }

  /**
   * Periodically polls for the result of the jobs and updates metadata accordingly.
   */
  @NotThreadSafe
  private final class PersistenceChecker implements alluxio.heartbeat.HeartbeatExecutor {

    /**
     * Creates a new instance of {@link PersistenceChecker}.
     */
    PersistenceChecker() {}

    @Override
    public void close() {} // nothing to clean up

    /**
     * Updates the file system metadata to reflect the fact that the persist job succeeded.
     *
     * NOTE: It is the responsibility of the caller to update {@link #mPersistJobs}.
     *
     * @param job the successful job
     */
    private void handleSuccess(PersistJob job) {
      long fileId = job.getFileId();
      String tempUfsPath = job.getTempUfsPath();
      List<Long> blockIds = new ArrayList<>();
      UfsManager.UfsClient ufsClient = null;
      try (JournalContext journalContext = createJournalContext();
          LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(fileId, LockPattern.WRITE_INODE)) {
        InodeFile inode = inodePath.getInodeFile();
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        ufsClient = mUfsManager.get(resolution.getMountId());
        switch (inode.getPersistenceState()) {
          case LOST:
            // fall through
          case NOT_PERSISTED:
            // fall through
          case PERSISTED:
            LOG.warn("File {} (id={}) persistence state is {}. Successful persist has no effect.",
                job.getUri(), fileId, inode.getPersistenceState());
            break;
          case TO_BE_PERSISTED:
            try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
              UnderFileSystem ufs = ufsResource.get();
              String ufsPath = resolution.getUri().toString();
              if (!ufs.renameRenamableFile(tempUfsPath, ufsPath)) {
                throw new IOException(
                    String.format("Failed to rename %s to %s.", tempUfsPath, ufsPath));
              }
              ufs.setOwner(ufsPath, inode.getOwner(), inode.getGroup());
              ufs.setMode(ufsPath, inode.getMode());
            }

            mInodeTree.updateInodeFile(journalContext, UpdateInodeFileEntry.newBuilder()
                .setId(inode.getId())
                .setPersistJobId(Constants.PERSISTENCE_INVALID_JOB_ID)
                .setTempUfsPath(Constants.PERSISTENCE_INVALID_UFS_PATH)
                .build());
            mInodeTree.updateInode(journalContext, UpdateInodeEntry.newBuilder()
                .setId(inode.getId())
                .setPersistenceState(PersistenceState.PERSISTED.name())
                .build());
            propagatePersistedInternal(journalContext, inodePath);
            Metrics.FILES_PERSISTED.inc();

            // Save state for possible cleanup
            blockIds.addAll(inode.getBlockIds());
            break;
          default:
            throw new IllegalStateException(
                "Unrecognized persistence state: " + inode.getPersistenceState());
        }
      } catch (FileDoesNotExistException | InvalidPathException e) {
        LOG.warn("The file {} (id={}) to be persisted was not found: {}", job.getUri(), fileId,
            e.getMessage());
        LOG.debug("Exception: ", e);
        // Cleanup the temporary file.
        if (ufsClient != null) {
          try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
            cleanup(ufsResource.get(), tempUfsPath);
          }
        }
      } catch (Exception e) {
        LOG.warn(
            "Unexpected exception encountered when trying to complete persistence of a file {} "
                + "(id={}) : {}",
            job.getUri(), fileId, e.getMessage());
        LOG.debug("Exception: ", e);
        if (ufsClient != null) {
          try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
            cleanup(ufsResource.get(), tempUfsPath);
          }
        }
        mPersistRequests.put(fileId, job.getTimer());
      } finally {
        mPersistJobs.remove(fileId);
      }

      // Cleanup possible staging UFS blocks files due to fast durable write fallback.
      // Note that this is best effort
      if (ufsClient != null) {
        for (long blockId : blockIds) {
          String ufsBlockPath = alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, blockId);
          try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
            alluxio.util.UnderFileSystemUtils.deleteFileIfExists(ufsResource.get(), ufsBlockPath);
          } catch (Exception e) {
            LOG.warn("Failed to clean up staging UFS block file {}", ufsBlockPath, e.getMessage());
          }
        }
      }
    }

    @Override
    public void heartbeat() throws InterruptedException {
      boolean queueEmpty = mPersistCheckerPool.getQueue().isEmpty();
      // Check the progress of persist jobs.
      for (long fileId : mPersistJobs.keySet()) {
        final PersistJob job = mPersistJobs.get(fileId);
        if (job == null) {
          // This could happen if a key is removed from mPersistJobs while we are iterating.
          continue;
        }
        // Cancel any jobs marked as canceled
        switch (job.getCancelState()) {
          case NOT_CANCELED:
            break;
          case TO_BE_CANCELED:
            // Send the message to cancel this job
            JobMasterClient client = mJobMasterClientPool.acquire();
            try {
              client.cancel(job.getId());
              job.setCancelState(PersistJob.CancelState.CANCELING);
            } catch (alluxio.exception.status.NotFoundException e) {
              LOG.warn("Persist job (id={}) for file {} (id={}) to cancel was not found: {}",
                  job.getId(), job.getUri(), fileId, e.getMessage());
              LOG.debug("Exception: ", e);
              mPersistJobs.remove(fileId);
              continue;
            } catch (Exception e) {
              LOG.warn("Unexpected exception encountered when cancelling a persist job (id={}) for "
                  + "file {} (id={}) : {}", job.getId(), job.getUri(), fileId, e.getMessage());
              LOG.debug("Exception: ", e);
            } finally {
              mJobMasterClientPool.release(client);
            }
            continue;
          case CANCELING:
            break;
          default:
            throw new IllegalStateException("Unrecognized cancel state: " + job.getCancelState());
        }
        if (!queueEmpty) {
          // There are tasks waiting in the queue, so do not try to schedule anything
          continue;
        }
        long jobId = job.getId();
        JobMasterClient client = mJobMasterClientPool.acquire();
        try {
          alluxio.job.wire.JobInfo jobInfo = client.getStatus(jobId);
          switch (jobInfo.getStatus()) {
            case RUNNING:
              // fall through
            case CREATED:
              break;
            case FAILED:
              LOG.warn("The persist job (id={}) for file {} (id={}) failed: {}", jobId,
                  job.getUri(), fileId, jobInfo.getErrorMessage());
              mPersistJobs.remove(fileId);
              mPersistRequests.put(fileId, job.getTimer());
              break;
            case CANCELED:
              mPersistJobs.remove(fileId);
              break;
            case COMPLETED:
              mPersistCheckerPool.execute(() -> handleSuccess(job));
              break;
            default:
              throw new IllegalStateException("Unrecognized job status: " + jobInfo.getStatus());
          }
        } catch (Exception e) {
          LOG.warn("Exception encountered when trying to retrieve the status of a "
                  + " persist job (id={}) for file {} (id={}): {}.", jobId, job.getUri(), fileId,
              e.getMessage());
          LOG.debug("Exception: ", e);
          mPersistJobs.remove(fileId);
          mPersistRequests.put(fileId, job.getTimer());
        } finally {
          mJobMasterClientPool.release(client);
        }
      }
    }
  }

  @NotThreadSafe
  private final class TimeSeriesRecorder implements alluxio.heartbeat.HeartbeatExecutor {
    @Override
    public void heartbeat() throws InterruptedException {
      // TODO(calvin): Provide a better way to keep track of metrics collected as time series
      MetricRegistry registry = MetricsSystem.METRIC_REGISTRY;

      // % Alluxio space used
      Long masterCapacityTotal = (Long) registry.getGauges()
          .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_TOTAL)).getValue();
      Long masterCapacityUsed = (Long) registry.getGauges()
          .get(MetricsSystem.getMetricName(DefaultBlockMaster.Metrics.CAPACITY_USED)).getValue();
      int percentAlluxioSpaceUsed =
          (masterCapacityTotal > 0) ? (int) (100L * masterCapacityUsed / masterCapacityTotal) : 0;
      mTimeSeriesStore.record("% Alluxio Space Used", percentAlluxioSpaceUsed);

      // % UFS space used
      Long masterUnderfsCapacityTotal =
          (Long) registry.getGauges()
              .get(MetricsSystem.getMetricName(MasterMetrics.UFS_CAPACITY_TOTAL)).getValue();
      Long masterUnderfsCapacityUsed =
          (Long) registry.getGauges()
              .get(MetricsSystem.getMetricName(MasterMetrics.UFS_CAPACITY_USED)).getValue();
      int percentUfsSpaceUsed =
          (masterUnderfsCapacityTotal > 0) ? (int) (100L * masterUnderfsCapacityUsed
              / masterUnderfsCapacityTotal) : 0;
      mTimeSeriesStore.record("% UFS Space Used", percentUfsSpaceUsed);
    }

    @Override
    public void close() {} // Nothing to clean up.
  }

  private static void cleanup(UnderFileSystem ufs, String ufsPath) {
    final String errMessage = "Failed to delete UFS file {}.";
    if (!ufsPath.isEmpty()) {
      try {
        if (!ufs.deleteExistingFile(ufsPath)) {
          LOG.warn(errMessage, ufsPath);
        }
      } catch (IOException e) {
        LOG.warn(errMessage, ufsPath, e);
      }
    }
  }

  @Override
  public void updateUfsMode(AlluxioURI ufsPath, UfsMode ufsMode) throws InvalidPathException,
      InvalidArgumentException, UnavailableException, AccessControlException {
    // TODO(adit): Create new fsadmin audit context
    try (RpcContext rpcContext = createRpcContext();
        FileSystemMasterAuditContext auditContext =
            createAuditContext("updateUfsMode", ufsPath, null, null)) {
      mUfsManager.setUfsMode(rpcContext, ufsPath, ufsMode);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Check if the specified operation type is allowed to the ufs.
   *
   * @param alluxioPath the Alluxio path
   * @param opType the operation type
   */
  private void checkUfsMode(AlluxioURI alluxioPath, OperationType opType)
      throws AccessControlException, InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioPath);
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      UfsMode ufsMode =
          ufs.getOperationMode(mUfsManager.getPhysicalUfsState(ufs.getPhysicalStores()));
      switch (ufsMode) {
        case NO_ACCESS:
          throw new AccessControlException(ExceptionMessage.UFS_OP_NOT_ALLOWED.getMessage(opType,
              resolution.getUri(), UfsMode.NO_ACCESS));
        case READ_ONLY:
          if (opType == OperationType.WRITE) {
            throw new AccessControlException(ExceptionMessage.UFS_OP_NOT_ALLOWED.getMessage(opType,
                resolution.getUri(), UfsMode.READ_ONLY));
          }
          break;
        default:
          // All operations are allowed
          break;
      }
    }
  }

  /**
   * The operation type. This class is used to check if an operation to the under storage is allowed
   * during maintenance.
   */
  enum OperationType {
    READ,
    WRITE,
  }

  /**
   * Class that contains metrics for FileSystemMaster.
   * This class is public because the counter names are referenced in
   * {@link alluxio.web.WebInterfaceAbstractMetricsServlet}.
   */
  public static final class Metrics {
    private static final Counter DIRECTORIES_CREATED
        = MetricsSystem.counter(MasterMetrics.DIRECTORIES_CREATED);
    private static final Counter FILE_BLOCK_INFOS_GOT
        = MetricsSystem.counter(MasterMetrics.FILE_BLOCK_INFOS_GOT);
    private static final Counter FILE_INFOS_GOT
        = MetricsSystem.counter(MasterMetrics.FILE_INFOS_GOT);
    private static final Counter FILES_COMPLETED
        = MetricsSystem.counter(MasterMetrics.FILES_COMPLETED);
    private static final Counter FILES_CREATED
        = MetricsSystem.counter(MasterMetrics.FILES_CREATED);
    private static final Counter FILES_FREED
        = MetricsSystem.counter(MasterMetrics.FILES_FREED);
    private static final Counter FILES_PERSISTED
        = MetricsSystem.counter(MasterMetrics.FILES_PERSISTED);
    private static final Counter NEW_BLOCKS_GOT
        = MetricsSystem.counter(MasterMetrics.NEW_BLOCKS_GOT);
    private static final Counter PATHS_DELETED
        = MetricsSystem.counter(MasterMetrics.PATHS_DELETED);
    private static final Counter PATHS_MOUNTED
        = MetricsSystem.counter(MasterMetrics.PATHS_MOUNTED);
    private static final Counter PATHS_RENAMED
        = MetricsSystem.counter(MasterMetrics.PATHS_RENAMED);
    private static final Counter PATHS_UNMOUNTED
        = MetricsSystem.counter(MasterMetrics.PATHS_UNMOUNTED);

    // TODO(peis): Increment the RPCs OPs at the place where we receive the RPCs.
    private static final Counter COMPLETE_FILE_OPS
        = MetricsSystem.counter(MasterMetrics.COMPLETE_FILE_OPS);
    private static final Counter CREATE_DIRECTORIES_OPS
        = MetricsSystem.counter(MasterMetrics.CREATE_DIRECTORIES_OPS);
    private static final Counter CREATE_FILES_OPS
        = MetricsSystem.counter(MasterMetrics.CREATE_FILES_OPS);
    private static final Counter DELETE_PATHS_OPS
        = MetricsSystem.counter(MasterMetrics.DELETE_PATHS_OPS);
    private static final Counter FREE_FILE_OPS
        = MetricsSystem.counter(MasterMetrics.FREE_FILE_OPS);
    private static final Counter GET_FILE_BLOCK_INFO_OPS
        = MetricsSystem.counter(MasterMetrics.GET_FILE_BLOCK_INFO_OPS);
    private static final Counter GET_FILE_INFO_OPS
        = MetricsSystem.counter(MasterMetrics.GET_FILE_INFO_OPS);
    private static final Counter GET_NEW_BLOCK_OPS
        = MetricsSystem.counter(MasterMetrics.GET_NEW_BLOCK_OPS);
    private static final Counter MOUNT_OPS
        = MetricsSystem.counter(MasterMetrics.MOUNT_OPS);
    private static final Counter RENAME_PATH_OPS
        = MetricsSystem.counter(MasterMetrics.RENAME_PATH_OPS);
    private static final Counter SET_ACL_OPS
        = MetricsSystem.counter(MasterMetrics.SET_ACL_OPS);
    private static final Counter SET_ATTRIBUTE_OPS
        = MetricsSystem.counter(MasterMetrics.SET_ATTRIBUTE_OPS);
    private static final Counter UNMOUNT_OPS
        = MetricsSystem.counter(MasterMetrics.UNMOUNT_OPS);

    /**
     * Register some file system master related gauges.
     *
     * @param master the file system master
     * @param ufsManager the under filesystem manager
     */
    @VisibleForTesting
    public static void registerGauges(
        final FileSystemMaster master, final UfsManager ufsManager) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem
              .getMetricName(MasterMetrics.FILES_PINNED),
          master::getNumberOfPinnedFiles);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem
              .getMetricName(MasterMetrics.TOTAL_PATHS),
          () -> master.getInodeCount());

      final String ufsDataFolder = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem
              .getMetricName(MasterMetrics.UFS_CAPACITY_TOTAL),
          () -> {
            try (CloseableResource<UnderFileSystem> ufsResource =
                ufsManager.getRoot().acquireUfsResource()) {
              UnderFileSystem ufs = ufsResource.get();
              return ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL);
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
              return Stream.empty();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem
              .getMetricName(MasterMetrics.UFS_CAPACITY_USED),
          () -> {
            try (CloseableResource<UnderFileSystem> ufsResource =
                ufsManager.getRoot().acquireUfsResource()) {
              UnderFileSystem ufs = ufsResource.get();
              return ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED);
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
              return Stream.empty();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem
              .getMetricName(MasterMetrics.UFS_CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try (CloseableResource<UnderFileSystem> ufsResource =
                       ufsManager.getRoot().acquireUfsResource()) {
                UnderFileSystem ufs = ufsResource.get();
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_FREE);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * Creates a {@link FileSystemMasterAuditContext} instance.
   *
   * @param command the command to be logged by this {@link AuditContext}
   * @param srcPath the source path of this command
   * @param dstPath the destination path of this command
   * @param srcInode the source inode of this command
   * @return newly-created {@link FileSystemMasterAuditContext} instance
   */
  private FileSystemMasterAuditContext createAuditContext(String command, AlluxioURI srcPath,
      @Nullable AlluxioURI dstPath, @Nullable Inode srcInode)
      throws AccessControlException {
    FileSystemMasterAuditContext auditContext =
        new FileSystemMasterAuditContext(mAsyncAuditLogWriter);
    if (mAsyncAuditLogWriter != null) {
      String user = AuthenticatedClientUser.getClientUser(ServerConfiguration.global());
      String ugi;
      try {
        String primaryGroup = CommonUtils.getPrimaryGroupName(user, ServerConfiguration.global());
        ugi = user + "," + primaryGroup;
      } catch (IOException e) {
        LOG.warn("Failed to get primary group for user {}.", user);
        ugi = user;
      }
      AuthType authType =
          ServerConfiguration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      auditContext.setUgi(ugi)
          .setAuthType(authType)
          .setIp(ClientIpAddressInjector.getIpAddress())
          .setCommand(command).setSrcPath(srcPath).setDstPath(dstPath)
          .setSrcInode(srcInode).setAllowed(true);
    }
    return auditContext;
  }

  private BlockDeletionContext createBlockDeletionContext() {
    return new DefaultBlockDeletionContext(this::removeBlocks,
        blocks -> blocks.forEach(mUfsBlockLocationCache::invalidate));
  }

  private void removeBlocks(List<Long> blocks) throws IOException {
    if (blocks.isEmpty()) {
      return;
    }
    RetryPolicy retry = new CountingRetry(3);
    IOException lastThrown = null;
    while (retry.attempt()) {
      try {
        mBlockMaster.removeBlocks(blocks, true);
        return;
      } catch (UnavailableException e) {
        lastThrown = e;
      }
    }
    throw new IOException("Failed to remove deleted blocks from block master", lastThrown);
  }

  /**
   * @return a context for executing an RPC
   */
  @VisibleForTesting
  public RpcContext createRpcContext() throws UnavailableException {
    return new RpcContext(createBlockDeletionContext(), createJournalContext());
  }

  private LockingScheme createLockingScheme(AlluxioURI path, FileSystemMasterCommonPOptions options,
      LockPattern desiredLockMode) {
    boolean shouldSync =
        mUfsSyncPathCache.shouldSyncPath(path.getPath(), options.getSyncIntervalMs());
    return new LockingScheme(path, desiredLockMode, shouldSync);
  }

  private boolean isAclEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED);
  }

  @Override
  public List<TimeSeries> getTimeSeries() {
    return mTimeSeriesStore.getTimeSeries();
  }
}
