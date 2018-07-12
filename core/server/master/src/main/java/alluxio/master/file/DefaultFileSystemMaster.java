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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.collections.Pair;
import alluxio.collections.PrefixList;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
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
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.ProtobufUtils;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.master.audit.AuditContext;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodePathPair;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockedInodePathList;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsBlockLocationCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.DescendantType;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.file.options.WorkerHeartbeatOptions;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.NoopJournalContext;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.File;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.File.AsyncPersistRequestEntry;
import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.File.DeleteFileEntry;
import alluxio.proto.journal.File.DeleteMountPointEntry;
import alluxio.proto.journal.File.InodeDirectoryEntry;
import alluxio.proto.journal.File.InodeFileEntry;
import alluxio.proto.journal.File.InodeLastModificationTimeEntry;
import alluxio.proto.journal.File.PersistDirectoryEntry;
import alluxio.proto.journal.File.ReinitializeFileEntry;
import alluxio.proto.journal.File.RenameEntry;
import alluxio.proto.journal.File.SetAttributeEntry;
import alluxio.proto.journal.File.StringPairEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.resource.CloseableResource;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemCommandOptions;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.MountTOptions;
import alluxio.thrift.PersistCommandOptions;
import alluxio.thrift.PersistFile;
import alluxio.thrift.UfsInfo;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.Fingerprint.Tag;
import alluxio.underfs.MasterUfsManager;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.ListOptions;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.CommonOptions;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.MountPointInfo;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The master that handles all file system metadata management.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class DefaultFileSystemMaster extends AbstractMaster implements FileSystemMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultFileSystemMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(BlockMaster.class);

  /**
   * Locking in DefaultFileSystemMaster
   *
   * Individual paths are locked in the inode tree. In order to read or write any inode, the path
   * must be locked. A path is locked via one of the lock methods in {@link InodeTree}, such as
   * {@link InodeTree#lockInodePath(AlluxioURI, InodeTree.LockMode)} or
   * {@link InodeTree#lockFullInodePath(AlluxioURI, InodeTree.LockMode)}. These lock methods return
   * an {@link LockedInodePath}, which represents a locked path of inodes. These locked paths
   * ({@link LockedInodePath}) must be unlocked. In order to ensure a locked
   * {@link LockedInodePath} is always unlocked, the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
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
   *   (B) private (or package private) methods that journal
   *   (C) private (or package private) internal methods
   *   (D) private FromEntry methods used to replay entries from the journal
   *
   * (A) public api methods:
   * These methods are public and are accessed by the RPC and REST APIs. These methods lock all
   * the required paths, and also perform all permission checking.
   * (A) cannot call (A)
   * (A) can call (B)
   * (A) can call (C)
   * (A) cannot call (D)
   *
   * (B) private (or package private) methods that journal:
   * These methods perform the work from the public apis, like checking and error handling, and also
   * asynchronously append entries to the journal. The names of these methods are suffixed with
   * "AndJournal".
   * (B) cannot call (A)
   * (B) can call (B)
   * (B) can call (C)
   * (B) cannot call (D)
   *
   * (C) private (or package private) internal methods:
   * These methods perform the rest of the work, and may append to the journal. The names of these
   * methods are suffixed by "Internal". These are typically called by the (D) methods.
   * (C) cannot call (A)
   * (C) cannot call (B)
   * (C) can call (C)
   * (C) cannot call (D)
   *
   * (D) private FromEntry methods used to replay entries from the journal:
   * These methods are used to replay entries from reading the journal. This is done on start, as
   * well as for secondary masters. When replaying entries, no additional journaling should happen,
   * so {@link NoopJournalContext#INSTANCE} must be used if a context is necessary.
   * (D) cannot call (A)
   * (D) cannot call (B)
   * (D) can call (C)
   * (D) cannot call (D)
   */

  /** Handle to the block master. */
  private final BlockMaster mBlockMaster;

  /** This manages the file system inode structure. This must be journaled. */
  private final InodeTree mInodeTree;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /** This maintains inodes with ttl set, for the for the ttl checker service to use. */
  private final TtlBucketList mTtlBuckets = new TtlBucketList();

  /** This generates unique directory ids. This must be journaled. */
  private final InodeDirectoryIdGenerator mDirectoryIdGenerator;

  /** This checks user permissions on different operations. */
  private final PermissionChecker mPermissionChecker;

  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /** The handler for async persistence. */
  private final AsyncPersistHandler mAsyncPersistHandler;

  /** The manager of all ufs. */
  private final MasterUfsManager mUfsManager;

  /** This caches absent paths in the UFS. */
  private final UfsAbsentPathCache mUfsAbsentPathCache;

  /** This caches block locations in the UFS. */
  private final UfsBlockLocationCache mUfsBlockLocationCache;

  /** This caches paths which have been synced with UFS. */
  private final UfsSyncPathCache mUfsSyncPathCache;

  /**
   * The service that checks for inode files with ttl set. We store it here so that it can be
   * accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mTtlCheckerService;

  /**
   * The service that detects lost files. We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostFilesDetectionService;

  /**
   * The service that checks for blocks which no longer correspond to existing inodes.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mBlockIntegrityCheck;

  private Future<List<AlluxioURI>> mStartupConsistencyCheck;

  /**
   * Log writer for user access audit log.
   */
  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  /**
   * Creates a new instance of {@link DefaultFileSystemMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   */
  DefaultFileSystemMaster(BlockMaster blockMaster, MasterContext masterContext) {
    this(blockMaster, masterContext, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.FILE_SYSTEM_MASTER_NAME, 4));
  }

  /**
   * Creates a new instance of {@link DefaultFileSystemMaster}.
   *
   * @param blockMaster a block master handle
   * @param masterContext the context for Alluxio master
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultFileSystemMaster(BlockMaster blockMaster, MasterContext masterContext,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, new SystemClock(), executorServiceFactory);

    mBlockMaster = blockMaster;
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mUfsManager = new MasterUfsManager();
    mMountTable = new MountTable(mUfsManager);
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator, mMountTable);

    // TODO(gene): Handle default config value for whitelist.
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.MASTER_WHITELIST, ","));

    mAsyncPersistHandler = AsyncPersistHandler.Factory.create(new FileSystemMasterView(this));
    mPermissionChecker = new DefaultPermissionChecker(mInodeTree);
    mUfsAbsentPathCache = UfsAbsentPathCache.Factory.create(mMountTable);
    mUfsBlockLocationCache = UfsBlockLocationCache.Factory.create(mMountTable);
    mUfsSyncPathCache = new UfsSyncPathCache();

    resetState();
    Metrics.registerGauges(this, mUfsManager);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientServiceProcessor(
            new FileSystemMasterClientServiceHandler(this)));
    services.put(Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_NAME,
        new FileSystemMasterWorkerService.Processor<>(
            new FileSystemMasterWorkerServiceHandler(this)));
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
  public void processJournalEntry(JournalEntry entry) throws IOException {
    if (entry.hasInodeFile()) {
      mInodeTree.addInodeFileFromJournal(entry.getInodeFile());
      // Add the file to TTL buckets, the insert automatically rejects files w/ Constants.NO_TTL
      InodeFileEntry inodeFileEntry = entry.getInodeFile();
      if (inodeFileEntry.hasTtl()) {
        mTtlBuckets.insert(InodeFile.fromJournalEntry(inodeFileEntry));
      }
    } else if (entry.hasInodeDirectory()) {
      try {
        // Add the directory to TTL buckets, the insert automatically rejects directory
        // w/ Constants.NO_TTL
        InodeDirectoryEntry inodeDirectoryEntry = entry.getInodeDirectory();
        if (inodeDirectoryEntry.hasTtl()) {
          mTtlBuckets.insert(InodeDirectory.fromJournalEntry(inodeDirectoryEntry));
        }
        mInodeTree.addInodeDirectoryFromJournal(entry.getInodeDirectory());
      } catch (AccessControlException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasInodeLastModificationTime()) {
      InodeLastModificationTimeEntry modTimeEntry = entry.getInodeLastModificationTime();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(modTimeEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode()
            .setLastModificationTimeMs(modTimeEntry.getLastModificationTimeMs(), true);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasPersistDirectory()) {
      PersistDirectoryEntry typedEntry = entry.getPersistDirectory();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(typedEntry.getId(), InodeTree.LockMode.WRITE)) {
        inodePath.getInode().setPersistenceState(PersistenceState.PERSISTED);
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasCompleteFile()) {
      try {
        completeFileFromEntry(entry.getCompleteFile());
      } catch (InvalidPathException | InvalidFileSizeException | FileAlreadyCompletedException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasSetAttribute()) {
      try {
        setAttributeFromEntry(entry.getSetAttribute());
      } catch (AccessControlException | FileDoesNotExistException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteFile()) {
      deleteFromEntry(entry.getDeleteFile());
    } else if (entry.hasRename()) {
      renameFromEntry(entry.getRename());
    } else if (entry.hasInodeDirectoryIdGenerator()) {
      mDirectoryIdGenerator.initFromJournalEntry(entry.getInodeDirectoryIdGenerator());
    } else if (entry.hasReinitializeFile()) {
      resetBlockFileFromEntry(entry.getReinitializeFile());
    } else if (entry.hasAddMountPoint()) {
      try {
        mountFromEntry(entry.getAddMountPoint());
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasDeleteMountPoint()) {
      unmountFromEntry(entry.getDeleteMountPoint());
    } else if (entry.hasAsyncPersistRequest()) {
      long fileId = (entry.getAsyncPersistRequest()).getFileId();
      try (LockedInodePath inodePath = mInodeTree
          .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
        try {
          scheduleAsyncPersistenceInternal(inodePath);
        } catch (AlluxioException e) {
          // It's possible that rescheduling the async persist calls fails, because the blocks may
          // no longer be in the memory
          LOG.warn("Failed to reschedule async persistence for {}: {}", inodePath.getUri(),
              e.toString());
        }
      } catch (FileDoesNotExistException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasUpdateUfsMode()) {
      try {
        updateUfsModeFromEntry(entry.getUpdateUfsMode());
      } catch (AlluxioException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void resetState() {
    mInodeTree.reset();
    String rootUfsUri = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Map<String, String> rootUfsConf =
        Configuration.getNestedProperties(PropertyKey.MASTER_MOUNT_TABLE_ROOT_OPTION);
    mMountTable.clear();
    // Initialize the root mount if it doesn't exist yet.
    if (!mMountTable.isMountPoint(new AlluxioURI(MountTable.ROOT))) {
      try (CloseableResource<UnderFileSystem> ufsResource =
          mUfsManager.getRoot().acquireUfsResource()) {
        // The root mount is a part of the file system master's initial state. The mounting is not
        // journaled, so the root will be re-mounted based on configuration whenever the master
        // starts.
        long rootUfsMountId = IdUtils.ROOT_MOUNT_ID;
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(rootUfsUri), rootUfsMountId,
            MountOptions.defaults()
                .setShared(ufsResource.get().isObjectStorage() && Configuration
                    .getBoolean(PropertyKey.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY))
                .setProperties(rootUfsConf));
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    return Iterators.concat(mInodeTree.getJournalEntryIterator(),
        CommonUtils.singleElementIterator(mDirectoryIdGenerator.toJournalEntry()),
        // The mount table should be written to the checkpoint after the inodes are written, so that
        // when replaying the checkpoint, the inodes exist before mount entries. Replaying a mount
        // entry traverses the inode tree.
        mMountTable.getJournalEntryIterator());
  }

  @Override
  public void start(Boolean isPrimary) throws IOException {
    super.start(isPrimary);
    if (isPrimary) {
      LOG.info("Starting fs master as primary");

      InodeDirectory root = mInodeTree.getRoot();
      if (root == null) {
        try (JournalContext context = createJournalContext()) {
          mInodeTree.initializeRoot(SecurityUtils.getOwnerFromLoginModule(),
              SecurityUtils.getGroupFromLoginModule(),
              Mode.createFullAccess().applyDirectoryUMask());
          context.append(mInodeTree.getRoot().toJournalEntry());
        }
      } else {
        // For backwards-compatibility:
        // Empty root owner indicates that previously the master had no security. In this case, the
        // master is allowed to be started with security turned on.
        String serverOwner = SecurityUtils.getOwnerFromLoginModule();
        if (SecurityUtils.isSecurityEnabled() && !root.getOwner().isEmpty()
            && !root.getOwner().equals(serverOwner)) {
          // user is not the previous owner
          throw new PermissionDeniedException(ExceptionMessage.PERMISSION_DENIED.getMessage(String
              .format("Unauthorized user on root. inode owner: %s current user: %s",
                  root.getOwner(), serverOwner)));
        }
      }

      // Startup Checks and Periodic Threads.
      if (Configuration.getBoolean(PropertyKey.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED)) {
        validateInodeBlocks(true);
      }

      int blockIntegrityCheckInterval =
          (int) Configuration.getMs(PropertyKey.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_INTERVAL);
      if (blockIntegrityCheckInterval > 0) { // negative or zero interval implies disabled
        mBlockIntegrityCheck = getExecutorService().submit(
            new HeartbeatThread(HeartbeatContext.MASTER_BLOCK_INTEGRITY_CHECK,
                new BlockIntegrityChecker(this), blockIntegrityCheckInterval));
      }
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK,
              new InodeTtlChecker(this, mInodeTree, mTtlBuckets),
              (int) Configuration.getMs(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
              new LostFileDetector(this, mInodeTree),
              (int) Configuration.getMs(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL)));
      if (Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
        mStartupConsistencyCheck = getExecutorService().submit(() -> startupCheckConsistency(
            ExecutorServiceFactories
               .fixedThreadPoolExecutorServiceFactory("startup-consistency-check", 32).create()));
      }
      if (Configuration.getBoolean(PropertyKey.MASTER_AUDIT_LOGGING_ENABLED)) {
        mAsyncAuditLogWriter = new AsyncUserAccessAuditLogWriter();
        mAsyncAuditLogWriter.start();
      }
    }
  }

  @Override
  public void stop() throws IOException {
    if (mAsyncAuditLogWriter != null) {
      mAsyncAuditLogWriter.stop();
      mAsyncAuditLogWriter = null;
    }
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
   * @throws InterruptedException if the thread is interrupted during execution
   */
  private List<AlluxioURI> startupCheckConsistency(final ExecutorService service)
      throws InterruptedException, IOException {
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
        try (LockedInodePath dir = mInodeTree.lockFullInodePath(mFileId, InodeTree.LockMode.READ)) {
          Inode parentInode = dir.getInode();
          AlluxioURI parentUri = dir.getUri();
          if (!checkConsistencyInternal(parentInode, parentUri)) {
            inconsistentUris.add(parentUri);
          }
          for (Inode childInode : ((InodeDirectory) parentInode).getChildren()) {
            try {
              childInode.lockReadAndCheckParent(parentInode);
            } catch (InvalidPathException e) {
              // This should be safe, continue.
              LOG.debug("Error during startup check consistency, ignoring and continuing.", e);
              continue;
            }
            try {
              AlluxioURI childUri = parentUri.join(childInode.getName());
              if (childInode.isDirectory()) {
                dirsToCheck.add(childInode.getId());
              } else {
                if (!checkConsistencyInternal(childInode, childUri)) {
                  inconsistentUris.add(childUri);
                }
              }
            } finally {
              childInode.unlockRead();
            }
          }
        } catch (FileDoesNotExistException e) {
          // This should be safe, continue.
          LOG.debug("A file scheduled for consistency check was deleted before the check.");
        } catch (InvalidPathException e) {
          // This should not happen.
          LOG.error("An invalid path was discovered during the consistency check, skipping.", e);
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
  public StartupConsistencyCheck getStartupConsistencyCheck() {
    if (!Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
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
         LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExistAndJournal(rpcContext, inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(true));
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return inodePath.getInode().getId();
    } catch (InvalidPathException | FileDoesNotExistException e) {
      return IdUtils.INVALID_FILE_ID;
    }
  }

  @Override
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return getFileInfoInternal(inodePath);
    }
  }

  @Override
  public FileInfo getFileInfo(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException, IOException {
    Metrics.GET_FILE_INFO_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.READ);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
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
        options.setLoadMetadataType(LoadMetadataType.Never);
      }

      // If the file already exists, then metadata does not need to be loaded,
      // otherwise load metadata.
      if (!inodePath.fullPathExists()) {
        checkLoadMetadataOptions(options.getLoadMetadataType(), inodePath.getUri());
        loadMetadataIfNotExistAndJournal(rpcContext, inodePath,
            LoadMetadataOptions.defaults().setCreateAncestors(true));
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
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  private FileInfo getFileInfoInternal(LockedInodePath inodePath)
      throws FileDoesNotExistException, AccessControlException, UnavailableException {
    Inode<?> inode = inodePath.getInode();
    AlluxioURI uri = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(uri.toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
    fileInfo.setInAlluxioPercentage(getInAlluxioPercentage(inode));
    if (inode instanceof InodeFile) {
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
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return inodePath.getInode().getPersistenceState();
    }
  }

  @Override
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException {
    Metrics.GET_FILE_INFO_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, listStatusOptions.getCommonOptions(), InodeTree.LockMode.READ);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("listStatus", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }

      DescendantType descendantType = listStatusOptions.isRecursive() ? DescendantType.ALL
          : DescendantType.ONE;
      // Possible ufs sync.
      if (syncMetadata(rpcContext, inodePath, lockingScheme, descendantType)) {
        // If synced, do not load metadata.
        listStatusOptions.setLoadMetadataType(LoadMetadataType.Never);
      }

      DescendantType loadDescendantType;
      if (listStatusOptions.getLoadMetadataType() == LoadMetadataType.Never) {
        loadDescendantType = DescendantType.NONE;
      } else if (listStatusOptions.isRecursive()) {
        loadDescendantType = DescendantType.ALL;
      } else {
        loadDescendantType = DescendantType.ONE;
      }
      // load metadata for 1 level of descendants, or all descendants if recursive
      LoadMetadataOptions loadMetadataOptions =
          LoadMetadataOptions.defaults().setCreateAncestors(true)
              .setLoadDescendantType(loadDescendantType);
      Inode<?> inode;
      if (inodePath.fullPathExists()) {
        inode = inodePath.getInode();
        if (inode.isDirectory()
            && listStatusOptions.getLoadMetadataType() != LoadMetadataType.Always) {
          InodeDirectory inodeDirectory = (InodeDirectory) inode;

          boolean isLoaded = inodeDirectory.isDirectChildrenLoaded();
          if (listStatusOptions.isRecursive()) {
            isLoaded = inodeDirectory.areDescendantsLoaded();
          }
          if (isLoaded) {
            // no need to load again.
            loadMetadataOptions.setLoadDescendantType(DescendantType.NONE);
          }
        }
      } else {
        checkLoadMetadataOptions(listStatusOptions.getLoadMetadataType(), inodePath.getUri());
      }

      loadMetadataIfNotExistAndJournal(rpcContext, inodePath, loadMetadataOptions);
      ensureFullPathAndUpdateCache(inodePath);
      inode = inodePath.getInode();
      auditContext.setSrcInode(inode);
      List<FileInfo> ret = new ArrayList<>();
      DescendantType descendantTypeForListStatus = (listStatusOptions.isRecursive())
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
   * @throws AccessControlException if the path can not be read by the user
   * @throws FileDoesNotExistException if the path does not exist
   * @throws UnavailableException if the service is temporarily unavailable
   */
  private void listStatusInternal(LockedInodePath currInodePath, AuditContext auditContext,
      DescendantType descendantType, List<FileInfo> statusList)
      throws FileDoesNotExistException, UnavailableException,
      AccessControlException, InvalidPathException {
    Inode<?> inode = currInodePath.getInode();
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
      String [] childComponents = null;
      if (!((InodeDirectory) inode).getChildren().isEmpty()) {
        String [] parentComponents = PathUtils.getPathComponents(currInodePath.getUri().getPath());
        childComponents = new String[parentComponents.length + 1];
        System.arraycopy(parentComponents, 0, childComponents, 0,  parentComponents.length);
      }

      for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
        // TODO(david): Make extending InodePath more efficient
        childComponents[childComponents.length - 1] = child.getName();

        try (LockedInodePath childInodePath  = mInodeTree.lockChildPath(currInodePath,
            InodeTree.LockMode.READ, child, childComponents)) {
          listStatusInternal(childInodePath, auditContext,
              nextDescendantType, statusList);
        }
      }
    }
    statusList.add(getFileInfoInternal(currInodePath));
  }

  /**
   * Checks the {@link LoadMetadataType} to determine whether or not to proceed in loading
   * metadata. This method assumes that the path does not exist in Alluxio namespace, and will
   * throw an exception if metadata should not be loaded.
   *
   * @param loadMetadataType the {@link LoadMetadataType} to check
   * @param path the path that does not exist in Alluxio namespace (used for exception message)
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  private void checkLoadMetadataOptions(LoadMetadataType loadMetadataType, AlluxioURI path)
      throws InvalidPathException, FileDoesNotExistException {
    if (loadMetadataType == LoadMetadataType.Never || (loadMetadataType == LoadMetadataType.Once
        && mUfsAbsentPathCache.isAbsent(path))) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
    }
  }

  /**
   * Checks to see if the entire path exists in Alluxio. Updates the absent cache if it does not
   * exist.
   *
   * @param inodePath the path to ensure
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  private void ensureFullPathAndUpdateCache(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException {
    boolean exists = false;
    try {
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
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
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException {
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.READ);
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath parent =
             mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
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

      if (!checkConsistencyInternal(parent.getInode(), parent.getUri())) {
        inconsistentUris.add(parent.getUri());
      }

      try (LockedInodePathList children =
               mInodeTree.lockDescendants(parent, InodeTree.LockMode.READ)) {
        for (LockedInodePath child : children.getInodePathList()) {
          AlluxioURI currentPath = child.getUri();
          if (!checkConsistencyInternal(child.getInode(), currentPath)) {
            inconsistentUris.add(currentPath);
          }
        }
      }

      auditContext.setSucceeded(true);
    }
    return inconsistentUris;
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
   * @param inode the inode to check
   * @param path the current path associated with the inode
   * @return true if the path is consistent, false otherwise
   * @throws FileDoesNotExistException if the path cannot be found in the Alluxio inode tree
   * @throws InvalidPathException if the path is not well formed
   */
  private boolean checkConsistencyInternal(Inode inode, AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    MountTable.Resolution resolution = mMountTable.resolve(path);
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
        InodeFile file = (InodeFile) inode;
        return ufs.isFile(ufsPath)
            && ufs.getFileStatus(ufsPath).getContentLength() == file.getLength();
      }
    }
  }

  @Override
  public void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException,
      UnavailableException {
    Metrics.COMPLETE_FILE_OPS.inc();
    // No need to syncMetadata before complete.
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE);
         FileSystemMasterAuditContext auditContext =
             createAuditContext("completeFile", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      // Even readonly mount points should be able to complete a file, for UFS reads in CACHE mode.
      completeFileAndJournal(rpcContext, inodePath, options);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to complete
   * @param options the method options
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if an invalid file size is encountered
   */
  private void completeFileAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      CompleteFileOptions options)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException, UnavailableException {
    Inode<?> inode = inodePath.getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_MUST_BE_FILE.getMessage(inodePath.getUri()));
    }

    InodeFile fileInode = (InodeFile) inode;
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
    long length = fileInode.isPersisted() ? options.getUfsLength() : inAlluxioLength;

    String ufsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
    if (fileInode.isPersisted()) {
      UfsStatus ufsStatus = options.getUfsStatus();
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

    completeFileInternal(fileInode.getBlockIds(), inodePath, length, options.getOperationTimeMs(),
        ufsFingerprint, false);
    CompleteFileEntry completeFileEntry =
        CompleteFileEntry.newBuilder().addAllBlockIds(fileInode.getBlockIds()).setId(inode.getId())
            .setLength(length).setOpTimeMs(options.getOperationTimeMs())
            .setUfsFingerprint(ufsFingerprint).build();
    rpcContext.journal(JournalEntry.newBuilder().setCompleteFile(completeFileEntry).build());
  }

  /**
   * @param blockIds the block ids to use
   * @param inodePath the {@link LockedInodePath} to complete
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param ufsFingerprint the ufs fingerprint
   * @param replayed whether the operation is a result of replaying the journal
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  private void completeFileInternal(List<Long> blockIds, LockedInodePath inodePath, long length,
      long opTimeMs, String ufsFingerprint, boolean replayed)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException, UnavailableException {
    InodeFile inode = inodePath.getInodeFile();
    inode.setBlockIds(blockIds);
    // Inode last modification time are initialized as currentTimeMillis() when they are created.
    // When we load metadata from UFS we might need to update it to a smaller timestamp which
    // requires setting override argument to true here.
    inode.setLastModificationTimeMs(opTimeMs, true);
    inode.setUfsFingerprint(ufsFingerprint);
    inode.complete(length);

    if (inode.isPersisted()) {
      if (!replayed) {
        // Commit all the file blocks (without locations) so the metadata for the block exists.
        long currLength = length;
        for (long blockId : inode.getBlockIds()) {
          long blockSize = Math.min(currLength, inode.getBlockSizeBytes());
          mBlockMaster.commitBlockInUFS(blockId, blockSize);
          currLength -= blockSize;
        }
      }
      // The path exists in UFS, so it is no longer absent
      mUfsAbsentPathCache.processExisting(inodePath.getUri());
    }
    Metrics.FILES_COMPLETED.inc();
  }

  /**
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  private void completeFileFromEntry(CompleteFileEntry entry) throws InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, UnavailableException {
    try (LockedInodePath inodePath =
        mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      completeFileInternal(entry.getBlockIdsList(), inodePath, entry.getLength(),
          entry.getOpTimeMs(), entry.getUfsFingerprint(), true);
    } catch (FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    Metrics.CREATE_FILES_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
        FileSystemMasterAuditContext auditContext =
            createAuditContext("createFile", path, null, inodePath.getParentInodeOrNull())) {
      if (options.isRecursive()) {
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
      if (options.isPersisted()) {
        // Check if ufs is writable
        checkUfsMode(path, OperationType.WRITE);
      }
      createFileAndJournal(rpcContext, inodePath, options);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the file to create
   * @param options method options
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information in encountered
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void createFileAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      CreateFileOptions options) throws FileAlreadyExistsException, BlockInfoException,
      FileDoesNotExistException, InvalidPathException, IOException {
    createFileInternal(rpcContext, inodePath, options);
  }

  /**
   * @param rpcContext the rpc context
   * @param inodePath the path to be created
   * @param options method options
   * @return {@link InodeTree.CreatePathResult} with the path creation result
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if invalid block information is encountered
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  InodeTree.CreatePathResult createFileInternal(RpcContext rpcContext, LockedInodePath inodePath,
      CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      FileDoesNotExistException {
    if (mWhitelist.inList(inodePath.getUri().toString())) {
      options.setCacheable(true);
    }
    InodeTree.CreatePathResult createResult = mInodeTree.createPath(rpcContext, inodePath, options);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode<?>> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);

    mTtlBuckets.insert(inode);

    if (options.isPersisted()) {
      // The path exists in UFS, so it is no longer absent. The ancestors exist in UFS, but the
      // actual file does not exist in UFS yet.
      mUfsAbsentPathCache.processExisting(inodePath.getUri().getParent());
    }

    Metrics.FILES_CREATED.inc();
    Metrics.DIRECTORIES_CREATED.inc();
    return createResult;
  }

  @Override
  public long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException, UnavailableException {
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      long id = mInodeTree.reinitializeFile(inodePath, blockSizeBytes, ttl, ttlAction);
      ReinitializeFileEntry reinitializeFile =
          ReinitializeFileEntry.newBuilder().setPath(path.getPath())
              .setBlockSizeBytes(blockSizeBytes).setTtl(ttl)
              .setTtlAction(ProtobufUtils.toProtobuf(ttlAction)).build();
      rpcContext.journal(JournalEntry.newBuilder().setReinitializeFile(reinitializeFile).build());
      return id;
    }
  }

  /**
   * @param entry the entry to use
   */
  private void resetBlockFileFromEntry(ReinitializeFileEntry entry) {
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(new AlluxioURI(entry.getPath()), InodeTree.LockMode.WRITE)) {
      mInodeTree.reinitializeFile(inodePath, entry.getBlockSizeBytes(), entry.getTtl(),
          ProtobufUtils.fromProtobuf(entry.getTtlAction()));
    } catch (InvalidPathException | FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_NEW_BLOCK_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE);
        FileSystemMasterAuditContext auditContext =
            createAuditContext("getNewBlockIdForFile", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      Metrics.NEW_BLOCKS_GOT.inc();
      long blockId = inodePath.getInodeFile().getNewBlockId();
      auditContext.setSucceeded(true);
      return blockId;
    }
  }

  @Override
  public Map<String, MountPointInfo> getMountTable() {
    SortedMap<String, MountPointInfo> mountPoints = new TreeMap<>();
    for (Map.Entry<String, MountInfo> mountPoint : mMountTable.getMountTable().entrySet()) {
      MountInfo mountInfo = mountPoint.getValue();
      MountPointInfo info = mountInfo.toMountPointInfo();
      try (CloseableResource<UnderFileSystem> ufsResource =
          mUfsManager.get(mountInfo.getMountId()).acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        info.setUfsType(ufs.getUnderFSType());
        try {
          info.setUfsCapacityBytes(
              ufs.getSpace(info.getUfsUri(), UnderFileSystem.SpaceType.SPACE_TOTAL));
        } catch (IOException e) {
          // pass
        }
        try {
          info.setUfsUsedBytes(
              ufs.getSpace(info.getUfsUri(), UnderFileSystem.SpaceType.SPACE_USED));
        } catch (IOException e) {
          // pass
        }
      } catch (UnavailableException | NotFoundException e) {
        // We should never reach here
        LOG.error("No UFS cached for {}", info, e);
        continue;
      }
      mountPoints.put(mountPoint.getKey(), info);
    }
    return mountPoints;
  }

  @Override
  public int getNumberOfPaths() {
    return mInodeTree.getSize();
  }

  @Override
  public int getNumberOfPinnedFiles() {
    return mInodeTree.getPinnedSize();
  }

  @Override
  public void delete(AlluxioURI path, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
      AccessControlException {
    List<Inode<?>> deletedInodes;
    Metrics.DELETE_PATHS_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath =
            mInodeTree.lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
        FileSystemMasterAuditContext auditContext =
            createAuditContext("delete", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      mMountTable.checkUnderWritableMountPoint(path);
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme,
          options.isRecursive() ? DescendantType.ALL : DescendantType.ONE);
      if (!inodePath.fullPathExists()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }

      deleteAndJournal(rpcContext, inodePath, options);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Deletes a given path.
   * <p>
   * Writes to the journal.
   *
   * This method does not delete blocks. Instead, it adds blocks to the blockDeletionContext so that
   * they can be deleted after the inode deletion journal entry has been written. We cannot delete
   * blocks earlier because the inode deletion may fail, leaving us with inode containing deleted
   * blocks.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path to delete
   * @param deleteOptions the method options
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  @VisibleForTesting
  public void deleteAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      DeleteOptions deleteOptions)
      throws InvalidPathException, FileDoesNotExistException, IOException,
      DirectoryNotEmptyException {
    long opTimeMs = System.currentTimeMillis();
    deleteInternal(rpcContext, inodePath, false, opTimeMs, deleteOptions);
  }

  /**
   * @param entry the entry to use
   */
  private void deleteFromEntry(DeleteFileEntry entry) {
    Metrics.DELETE_PATHS_OPS.inc();
    try (LockedInodePath inodePath =
        mInodeTree.lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      deleteInternal(RpcContext.NOOP, inodePath, true, entry.getOpTimeMs(), DeleteOptions.defaults()
          .setRecursive(entry.getRecursive()).setAlluxioOnly(entry.getAlluxioOnly()));
    } catch (Exception e) {
      throw new RuntimeException(e);
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
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @param deleteOptions the method optitions
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if the specified path is the root
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  private void deleteInternal(RpcContext rpcContext, LockedInodePath inodePath, boolean replayed,
      long opTimeMs, DeleteOptions deleteOptions) throws FileDoesNotExistException, IOException,
      DirectoryNotEmptyException, InvalidPathException {
    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Alluxio and UFS.
    if (!inodePath.fullPathExists()) {
      return;
    }
    Inode<?> inode = inodePath.getInode();
    if (inode == null) {
      return;
    }

    boolean recursive = deleteOptions.isRecursive();
    if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
      // true
      throw new DirectoryNotEmptyException(ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE,
          inode.getName());
    }
    if (mInodeTree.isRootId(inode.getId())) {
      // The root cannot be deleted.
      throw new InvalidPathException(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    }

    List<Pair<AlluxioURI, LockedInodePath>> delInodes = new ArrayList<>();

    Pair<AlluxioURI, LockedInodePath> inodePair = new Pair<>(inodePath.getUri(), inodePath);
    delInodes.add(inodePair);

    try (LockedInodePathList lockedInodePathList =
        mInodeTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
      // Traverse inodes top-down
      for (LockedInodePath lockedInodePath : lockedInodePathList.getInodePathList()) {
        Inode descendant  = lockedInodePath.getInode();
        AlluxioURI descendantPath = mInodeTree.getPath(descendant);
        Pair<AlluxioURI, LockedInodePath> descendantPair =
            new Pair<>(descendantPath, lockedInodePath);
        delInodes.add(descendantPair);
      }

      // Prepare to delete persisted inodes
      UfsDeleter ufsDeleter = NoopUfsDeleter.INSTANCE;
      if (!(replayed || deleteOptions.isAlluxioOnly())) {
        ufsDeleter = new SafeUfsDeleter(mMountTable, delInodes, deleteOptions);
      }

      // Inodes to delete from tree after attempting to delete from UFS
      List<Pair<AlluxioURI, LockedInodePath>> inodesToDelete = new ArrayList<>();
      // Inodes that are not safe for recursive deletes
      Set<Long> unsafeInodes = new HashSet<>();
      // Alluxio URIs (and the reason for failure) which could not be deleted
      List<Pair<String, String>> failedUris = new ArrayList<>();

      // We go through each inode, removing it from its parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (int i = delInodes.size() - 1; i >= 0; i--) {
        Pair<AlluxioURI, LockedInodePath> delInodePair = delInodes.get(i);
        AlluxioURI alluxioUriToDel = delInodePair.getFirst();
        Inode delInode = delInodePair.getSecond().getInode();

        String failureReason = null;
        if (unsafeInodes.contains(delInode.getId())) {
          failureReason = ExceptionMessage.DELETE_FAILED_DIR_NONEMPTY.getMessage();
        } else if (!replayed && delInode.isPersisted()) {
          try {
            // If this is a mount point, we have deleted all the children and can unmount it
            // TODO(calvin): Add tests (ALLUXIO-1831)
            if (mMountTable.isMountPoint(alluxioUriToDel)) {
              unmountInternal(alluxioUriToDel);
            } else {
              if (!(replayed || deleteOptions.isAlluxioOnly())) {
                try {
                  checkUfsMode(alluxioUriToDel, OperationType.WRITE);
                  // Attempt to delete node if all children were deleted successfully
                  ufsDeleter.delete(alluxioUriToDel, delInode);
                } catch (AccessControlException e) {
                  // In case ufs is not writable, we will still attempt to delete other entries
                  // if any as they may be from a different mount point
                  // TODO(adit): reason for failure is swallowed here
                  LOG.warn(e.getMessage());
                  failureReason = e.getMessage();
                } catch (IOException e) {
                  failureReason = e.getMessage();
                }
              }
            }
          } catch (InvalidPathException e) {
            LOG.warn("Failed to delete path from UFS: {}", e.getMessage());
          }
        }
        if (failureReason == null) {
          inodesToDelete.add(new Pair<>(alluxioUriToDel, delInodePair.getSecond()));
        } else {
          unsafeInodes.add(delInode.getId());
          // Propagate 'unsafe-ness' to parent as one of its descendants can't be deleted
          unsafeInodes.add(delInode.getParentId());
          failedUris.add(new Pair<>(alluxioUriToDel.toString(), failureReason));
        }
      }

      // Delete Inodes
      for (Pair<AlluxioURI, LockedInodePath> delInodePair : inodesToDelete) {
        Inode delInode = delInodePair.getSecond().getInode();
        LockedInodePath tempInodePath = delInodePair.getSecond();
        // Do not journal entries covered recursively for performance
        if (delInode.getId() == inode.getId() || unsafeInodes.contains(delInode.getParentId())) {
          mInodeTree.deleteInode(rpcContext, tempInodePath, opTimeMs, deleteOptions);
        } else {
          mInodeTree.deleteInode(
              new RpcContext(rpcContext.getBlockDeletionContext(), NoopJournalContext.INSTANCE),
              tempInodePath, opTimeMs, deleteOptions);
        }
      }

      if (!failedUris.isEmpty()) {
        Collection<String> messages = failedUris.stream()
            .map(pair -> String.format("%s (%s)", pair.getFirst(), pair.getSecond()))
            .collect(Collectors.toList());
        throw new FailedPreconditionException(
            ExceptionMessage.DELETE_FAILED_UFS.getMessage(StringUtils.join(messages, ", ")));
      }
    }

    Metrics.PATHS_DELETED.inc(delInodes.size());
  }

  @Override
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException {
    Metrics.GET_FILE_BLOCK_INFO_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.READ);
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
   * @throws InvalidPathException if the path of the given file is invalid
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
   * @throws InvalidPathException if the mount table is not able to resolve the file
   */
  private FileBlockInfo generateFileBlockInfo(LockedInodePath inodePath, BlockInfo blockInfo)
      throws InvalidPathException, FileDoesNotExistException {
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
    Inode root = mInodeTree.getRoot();
    // Root has no parent, lock directly.
    root.lockRead();
    try {
      getInAlluxioFilesInternal(mInodeTree.getRoot(), new AlluxioURI(AlluxioURI.SEPARATOR), files);
    } finally {
      root.unlockRead();
    }
    return files;
  }

  @Override
  public List<AlluxioURI> getInMemoryFiles() throws UnavailableException {
    List<AlluxioURI> files = new ArrayList<>();
    Inode root = mInodeTree.getRoot();
    // Root has no parent, lock directly.
    root.lockRead();
    try {
      getInMemoryFilesInternal(mInodeTree.getRoot(), new AlluxioURI(AlluxioURI.SEPARATOR), files);
    } finally {
      root.unlockRead();
    }
    return files;
  }

  /**
   * Adds in-Alluxio files to the array list passed in. This method assumes the inode passed in is
   * already read locked.
   *
   * @param inode the root of the subtree to search
   * @param uri the uri of the parent of the inode
   * @param files the list to accumulate the results in
   */
  private void getInAlluxioFilesInternal(Inode<?> inode, AlluxioURI uri, List<AlluxioURI> files)
      throws UnavailableException {
    AlluxioURI newUri = uri.join(inode.getName());
    if (inode.isFile()) {
      if (isFullyInAlluxio((InodeFile) inode)) {
        files.add(newUri);
      }
    } else {
      // This inode is a directory.
      Set<Inode<?>> children = ((InodeDirectory) inode).getChildren();
      for (Inode<?> child : children) {
        try {
          child.lockReadAndCheckParent(inode);
        } catch (InvalidPathException e) {
          // Inode is no longer part of this directory.
          continue;
        }
        try {
          getInAlluxioFilesInternal(child, newUri, files);
        } finally {
          child.unlockRead();
        }
      }
    }
  }

  /**
   * Adds in-memory files to the array list passed in. This method assumes the inode passed in is
   * already read locked.
   *
   * @param inode the root of the subtree to search
   * @param uri the uri of the parent of the inode
   * @param files the list to accumulate the results in
   */
  private void getInMemoryFilesInternal(Inode<?> inode, AlluxioURI uri, List<AlluxioURI> files)
      throws UnavailableException {
    AlluxioURI newUri = uri.join(inode.getName());
    if (inode.isFile()) {
      if (isFullyInMemory((InodeFile) inode)) {
        files.add(newUri);
      }
    } else {
      // This inode is a directory.
      Set<Inode<?>> children = ((InodeDirectory) inode).getChildren();
      for (Inode<?> child : children) {
        try {
          child.lockReadAndCheckParent(inode);
        } catch (InvalidPathException e) {
          // Inode is no longer part of this directory.
          continue;
        }
        try {
          getInMemoryFilesInternal(child, newUri, files);
        } finally {
          child.unlockRead();
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
  private int getInMemoryPercentage(Inode<?> inode) throws UnavailableException {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = (InodeFile) inode;

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
  private int getInAlluxioPercentage(Inode<?> inode) throws UnavailableException {
    if (!inode.isFile()) {
      return 0;
    }
    InodeFile inodeFile = (InodeFile) inode;

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
  public long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    LOG.debug("createDirectory {} ", path);
    Metrics.CREATE_DIRECTORIES_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
         FileSystemMasterAuditContext auditContext =
             createAuditContext("mkdir", path, null, inodePath.getParentInodeOrNull())) {
      if (options.isRecursive()) {
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
      if (options.isPersisted()) {
        checkUfsMode(path, OperationType.WRITE);
      }
      createDirectoryAndJournal(rpcContext, inodePath, options);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Creates a directory for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} of the directory
   * @param options method options
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException when the path is invalid
   * @throws AccessControlException if permission checking fails
   */
  private void createDirectoryAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      CreateDirectoryOptions options) throws FileAlreadyExistsException, FileDoesNotExistException,
      InvalidPathException, AccessControlException, IOException {
    createDirectoryInternal(rpcContext, inodePath, options);
    Metrics.DIRECTORIES_CREATED.inc();
  }

  /**
   * Implementation of directory creation for a given path.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path of the directory
   * @param options method options
   * @return an {@link alluxio.master.file.meta.InodeTree.CreatePathResult} representing the
   *         modified inodes and created inodes during path creation
   * @throws InvalidPathException when the path is invalid
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws AccessControlException if permission checking fails
   */
  private InodeTree.CreatePathResult createDirectoryInternal(RpcContext rpcContext,
      LockedInodePath inodePath, CreateDirectoryOptions options) throws InvalidPathException,
      FileAlreadyExistsException, IOException, AccessControlException, FileDoesNotExistException {
    try {
      InodeTree.CreatePathResult createResult =
          mInodeTree.createPath(rpcContext, inodePath, options);
      InodeDirectory inodeDirectory = (InodeDirectory) inodePath.getInode();

      String ufsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
      if (inodeDirectory.isPersisted()) {
        UfsStatus ufsStatus = options.getUfsStatus();
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
      inodeDirectory.setUfsFingerprint(ufsFingerprint);

      // If inodeDirectory's ttl not equals Constants.NO_TTL, it should insert into mTtlBuckets
      if (createResult.getCreated().size() > 0) {
        mTtlBuckets.insert(inodeDirectory);
      }

      if (options.isPersisted()) {
        // The path exists in UFS, so it is no longer absent.
        mUfsAbsentPathCache.processExisting(inodePath.getUri());
      }

      return createResult;
    } catch (BlockInfoException e) {
      // Since we are creating a directory, the block size is ignored, no such exception should
      // happen.
      throw new RuntimeException(e);
    }
  }

  @Override
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.RENAME_PATH_OPS.inc();
    LockingScheme srcLockingScheme =
        createLockingScheme(srcPath, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    LockingScheme dstLockingScheme =
        createLockingScheme(dstPath, options.getCommonOptions(), InodeTree.LockMode.READ);
    // Require a WRITE lock on the source but only a READ lock on the destination. Since the
    // destination should not exist, we will only obtain a READ lock on the destination parent. The
    // modify operations on the parent inodes are thread safe so WRITE locks are not required.
    try (RpcContext rpcContext = createRpcContext();
         InodePathPair inodePathPair = mInodeTree
             .lockInodePathPair(srcLockingScheme.getPath(), srcLockingScheme.getMode(),
                 dstLockingScheme.getPath(), dstLockingScheme.getMode());
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
      renameAndJournal(rpcContext, srcInodePath, dstInodePath, options);
      auditContext.setSrcInode(srcInodePath.getInode()).setSucceeded(true);
      LOG.debug("Renamed {} to {}", srcPath, dstPath);
    }
  }

  /**
   * Renames a file to a destination.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param srcInodePath the source path to rename
   * @param dstInodePath the destination path to rename the file to
   * @param options method options
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws FileAlreadyExistsException if the file already exists
   */
  private void renameAndJournal(RpcContext rpcContext, LockedInodePath srcInodePath,
      LockedInodePath dstInodePath, RenameOptions options) throws InvalidPathException,
      FileDoesNotExistException, FileAlreadyExistsException, IOException, AccessControlException {
    if (!srcInodePath.fullPathExists()) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(srcInodePath.getUri()));
    }

    Inode<?> srcInode = srcInodePath.getInode();
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
    Inode<?> srcParentInode = srcInodePath.getParentInodeDirectory();
    if (!srcParentInode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(srcInodePath.getUri()));
    }
    Inode<?> dstParentInode = dstInodePath.getParentInodeDirectory();
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
    renameInternal(rpcContext, srcInodePath, dstInodePath, false, options);

    RenameEntry rename =
        RenameEntry.newBuilder().setId(srcInode.getId()).setDstPath(dstInodePath.getUri().getPath())
            .setOpTimeMs(options.getOperationTimeMs()).build();
    rpcContext.journal(JournalEntry.newBuilder().setRename(rename).build());
  }

  /**
   * Implements renaming.
   *
   * @param rpcContext the rpc context
   * @param srcInodePath the path of the rename source
   * @param dstInodePath the path to the rename destination
   * @param replayed whether the operation is a result of replaying the journal
   * @param options method options
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void renameInternal(RpcContext rpcContext, LockedInodePath srcInodePath,
      LockedInodePath dstInodePath, boolean replayed, RenameOptions options)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {

    // Rename logic:
    // 1. Change the source inode name to the destination name.
    // 2. Insert the source inode into the destination parent.
    // 3. Do UFS operations if necessary.
    // 4. Remove the source inode (reverting the name) from the source parent.
    // 5. Set the last modification times for both source and destination parent inodes.

    Inode<?> srcInode = srcInodePath.getInode();
    AlluxioURI srcPath = srcInodePath.getUri();
    AlluxioURI dstPath = dstInodePath.getUri();
    InodeDirectory srcParentInode = srcInodePath.getParentInodeDirectory();
    InodeDirectory dstParentInode = dstInodePath.getParentInodeDirectory();
    String srcName = srcPath.getName();
    String dstName = dstPath.getName();

    LOG.debug("Renaming {} to {}", srcPath, dstPath);

    // 1. Change the source inode name to the destination name.
    srcInode.setName(dstName);
    srcInode.setParentId(dstParentInode.getId());

    // 2. Insert the source inode into the destination parent.
    if (!dstParentInode.addChild(srcInode)) {
      // On failure, revert changes and throw exception.
      srcInode.setName(srcName);
      srcInode.setParentId(srcParentInode.getId());
      throw new InvalidPathException("Destination path: " + dstPath + " already exists.");
    }

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
        List<Inode<?>> dstInodeList = dstInodePath.getInodeList();
        for (int i = dstInodeList.size() - 1; i >= 0; i--) {
          // Since dstInodePath is guaranteed not to be a full path, all inodes in the incomplete
          // path are guaranteed to be a directory.
          InodeDirectory dir = (InodeDirectory) dstInodeList.get(i);
          sameMountDirs.push(dir);
          if (dir.isMountPoint()) {
            break;
          }
        }
        while (!sameMountDirs.empty()) {
          InodeDirectory dir = sameMountDirs.pop();
          if (!dir.isPersisted()) {
            mInodeTree.syncPersistDirectory(rpcContext, dir);
          }
        }

        String ufsSrcPath = resolution.getUri().toString();
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          String ufsDstUri = mMountTable.resolve(dstPath).getUri().toString();
          boolean success;
          if (srcInode.isFile()) {
            success = ufs.renameFile(ufsSrcPath, ufsDstUri);
          } else {
            success = ufs.renameDirectory(ufsSrcPath, ufsDstUri);
          }
          if (!success) {
            throw new IOException(
                ExceptionMessage.FAILED_UFS_RENAME.getMessage(ufsSrcPath, ufsDstUri));
          }
        }
        // The destination was persisted in ufs.
        mUfsAbsentPathCache.processExisting(dstPath);
      }
    } catch (Exception e) {
      // On failure, revert changes and throw exception.
      if (!dstParentInode.removeChild(dstName)) {
        LOG.error("Failed to revert rename changes. Alluxio metadata may be inconsistent.");
      }
      srcInode.setName(srcName);
      srcInode.setParentId(srcParentInode.getId());
      throw e;
    }

    // TODO(jiri): A crash between now and the time the rename operation is journaled will result in
    // an inconsistency between Alluxio and UFS.

    // 4. Remove the source inode (reverting the name) from the source parent. The name must be
    // reverted or removeChild will not be able to find the appropriate child entry since it is
    // keyed on the original name.
    srcInode.setName(srcName);
    if (!srcParentInode.removeChild(srcInode)) {
      // This should never happen.
      LOG.error("Failed to rename {} to {} in Alluxio. Alluxio and under storage may be "
          + "inconsistent.", srcPath, dstPath);
      srcInode.setName(dstName);
      if (!dstParentInode.removeChild(dstName)) {
        LOG.error("Failed to revert changes when renaming {} to {}. Alluxio metadata may be "
            + "inconsistent.", srcPath, dstPath);
      }
      srcInode.setName(srcName);
      srcInode.setParentId(srcParentInode.getId());
      throw new IOException("Failed to remove source path " + srcPath + " from parent");
    }
    srcInode.setName(dstName);

    // 5. Set the last modification times for both source and destination parent inodes.
    // Note this step relies on setLastModificationTimeMs being thread safe to guarantee the
    // correct behavior when multiple files are being renamed within a directory.
    dstParentInode.setLastModificationTimeMs(options.getOperationTimeMs());
    srcParentInode.setLastModificationTimeMs(options.getOperationTimeMs());
    Metrics.PATHS_RENAMED.inc();
  }

  /**
   * @param entry the entry to use
   */
  private void renameFromEntry(RenameEntry entry) {
    Metrics.RENAME_PATH_OPS.inc();
    // Determine the srcPath and dstPath
    AlluxioURI srcPath;
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.READ)) {
      srcPath = inodePath.getUri();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    AlluxioURI dstPath = new AlluxioURI(entry.getDstPath());

    // Both src and dst paths should lock WRITE_PARENT, to modify the parent inodes for both paths.
    try (InodePathPair inodePathPair = mInodeTree
        .lockInodePathPair(srcPath, InodeTree.LockMode.WRITE_PARENT, dstPath,
            InodeTree.LockMode.WRITE_PARENT)) {
      LockedInodePath srcInodePath = inodePathPair.getFirst();
      LockedInodePath dstInodePath = inodePathPair.getSecond();
      RenameOptions options = RenameOptions.defaults().setOperationTimeMs(entry.getOpTimeMs());
      renameInternal(RpcContext.NOOP, srcInodePath, dstInodePath, true, options);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Propagates the persisted status to all parents of the given inode in the same mount partition.
   *
   * @param inodePath the inode to start the propagation at
   * @param replayed whether the invocation is a result of replaying the journal
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException if a non-existent file is encountered
   */
  private List<Inode<?>> propagatePersistedInternal(LockedInodePath inodePath, boolean replayed)
      throws FileDoesNotExistException {
    Inode<?> inode = inodePath.getInode();
    if (!inode.isPersisted()) {
      return Collections.emptyList();
    }

    List<Inode<?>> inodes = inodePath.getInodeList();
    // Traverse the inodes from target inode to the root.
    Collections.reverse(inodes);
    // Skip the first, to not examine the target inode itself.
    inodes = inodes.subList(1, inodes.size());

    List<Inode<?>> persistedInodes = new ArrayList<>();
    for (Inode<?> handle : inodes) {
      // the path is already locked.
      AlluxioURI path = mInodeTree.getPath(handle);
      if (mMountTable.isMountPoint(path)) {
        // Stop propagating the persisted status at mount points.
        break;
      }
      if (handle.isPersisted()) {
        // Stop if a persisted directory is encountered.
        break;
      }
      handle.setPersistenceState(PersistenceState.PERSISTED);
      if (!replayed) {
        persistedInodes.add(handle);
      }
    }
    return persistedInodes;
  }

  /**
   * Journals the list of persisted inodes returned from
   * {@link #propagatePersistedInternal(LockedInodePath, boolean)}. This does not flush the journal.
   *
   * @param persistedInodes the list of persisted inodes to journal
   * @param journalContext the journal context
   */
  private void journalPersistedInodes(List<Inode<?>> persistedInodes,
      JournalContext journalContext) {
    for (Inode<?> inode : persistedInodes) {
      PersistDirectoryEntry persistDirectory =
          PersistDirectoryEntry.newBuilder().setId(inode.getId()).build();
      journalContext
          .append(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build());
    }
  }

  @Override
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, IOException {
    Metrics.FREE_FILE_OPS.inc();
    // No need to syncMetadata before free.
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE);
        FileSystemMasterAuditContext auditContext =
             createAuditContext("free", path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      freeAndJournal(rpcContext, inodePath, options);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Implements free operation.
   * <p>
   * This may write to the journal as free operation may change the pinned state of inodes.
   *
   * @param rpcContext the rpc context
   * @param inodePath inode of the path to free
   * @param options options to free
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  private void freeAndJournal(RpcContext rpcContext, LockedInodePath inodePath, FreeOptions options)
      throws FileDoesNotExistException, UnexpectedAlluxioException,
      IOException, InvalidPathException, AccessControlException {
    Inode<?> inode = inodePath.getInode();
    if (inode.isDirectory() && !options.isRecursive()
        && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't free a nonempty directory unless recursive is true
      throw new UnexpectedAlluxioException(
          ExceptionMessage.CANNOT_FREE_NON_EMPTY_DIR.getMessage(mInodeTree.getPath(inode)));
    }
    long opTimeMs = System.currentTimeMillis();
    List<Inode<?>> freeInodes = new ArrayList<>();
    freeInodes.add(inode);
    List<LockedInodePath> descendants = mInodeTree.lockDescendants(inodePath,
        InodeTree.LockMode.WRITE).getInodePathList();
    Closer closer = Closer.create();
    // Using a closer here because we add the inodePath to the descendants list which does not
    // need to be closed
    try {
      for (LockedInodePath descedant : descendants) {
        closer.register(descedant);
      }
      descendants.add(inodePath);
      for (LockedInodePath descedant : descendants) {
        Inode<?> freeInode = descedant.getInodeOrNull();

        if (freeInode != null && freeInode.isFile()) {
          if (freeInode.getPersistenceState() != PersistenceState.PERSISTED) {
            throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
                .getMessage(mInodeTree.getPath(freeInode)));
          }
          if (freeInode.isPinned()) {
            if (!options.isForced()) {
              throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_PINNED_FILE
                  .getMessage(mInodeTree.getPath(freeInode)));
            }

            SetAttributeOptions setAttributeOptions =
                SetAttributeOptions.defaults().setRecursive(false).setPinned(false);
            setAttributeInternal(descedant, false, opTimeMs, setAttributeOptions);
            journalSetAttribute(descedant, opTimeMs, setAttributeOptions,
                rpcContext.getJournalContext());
          }
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds(), false /* delete */);
        }
      }
    } finally {
      closer.close();
    }

    Metrics.FILES_FREED.inc(freeInodes.size());
  }

  @Override
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
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
    return Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
  }

  @Override
  public UfsInfo getUfsInfo(long mountId) {
    MountInfo info = mMountTable.getMountInfo(mountId);
    if (info == null) {
      return new UfsInfo();
    }
    MountOptions options = info.getOptions();
    return new UfsInfo().setUri(info.getUfsUri().toString())
        .setProperties(new MountTOptions().setProperties(options.getProperties())
            .setReadOnly(options.isReadOnly()).setShared(options.isShared()));
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

  @Override
  public void reportLostFile(long fileId) throws FileDoesNotExistException, UnavailableException {
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      Inode<?> inode = inodePath.getInode();
      if (inode.isDirectory()) {
        LOG.warn("Reported file is a directory {}", inode);
        return;
      }

      List<Long> blockIds = new ArrayList<>();
      try {
        for (FileBlockInfo fileBlockInfo : getFileBlockInfoListInternal(inodePath)) {
          blockIds.add(fileBlockInfo.getBlockInfo().getBlockId());
        }
      } catch (InvalidPathException e) {
        LOG.info("Failed to get file info {}", fileId, e);
      }
      mBlockMaster.reportLostBlocks(blockIds);
      LOG.info("Reported file loss of blocks {}. Alluxio will recompute it: {}", blockIds, fileId);
    }
  }

  @Override
  public long loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, IOException, AccessControlException {
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE);
        FileSystemMasterAuditContext auditContext =
             createAuditContext("loadMetadata", path, null, inodePath.getParentInodeOrNull())) {
      if (options.isCreateAncestors()) {
        auditContext.setSrcInode(inodePath.getLastExistingInode());
      }
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      loadMetadataAndJournal(rpcContext, inodePath, options);
      auditContext.setSrcInode(inodePath.getInode()).setSucceeded(true);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param options the load metadata options
   * @throws InvalidPathException if invalid path is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws AccessControlException if permission checking fails
   */
  private void loadMetadataAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataOptions options)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException, AccessControlException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      if (options.getUfsStatus() == null && !ufs.exists(ufsUri.toString())) {
        // uri does not exist in ufs
        InodeDirectory inode = (InodeDirectory) inodePath.getInode();
        inode.setDirectChildrenLoaded(true);
        return;
      }
      boolean isFile;
      if (options.getUfsStatus() != null) {
        isFile = options.getUfsStatus().isFile();
      } else {
        isFile = ufs.isFile(ufsUri.toString());
      }
      if (isFile) {
        loadFileMetadataAndJournal(rpcContext, inodePath, resolution, options);
      } else {
        loadDirectoryMetadataAndJournal(rpcContext, inodePath, options);
        InodeDirectory inode = (InodeDirectory) inodePath.getInode();

        if (options.getLoadDescendantType() != DescendantType.NONE) {
          ListOptions listOptions = ListOptions.defaults();
          if (options.getLoadDescendantType() == DescendantType.ALL) {
            listOptions.setRecursive(true);
          } else {
            listOptions.setRecursive(false);
          }
          UfsStatus[] children = ufs.listStatus(ufsUri.toString(), listOptions);
          Arrays.sort(children, Comparator.comparing(UfsStatus::getName));

          for (UfsStatus childStatus : children) {
            if (PathUtils.isTemporaryFileName(childStatus.getName())) {
              continue;
            }
            AlluxioURI childURI = new AlluxioURI(
                PathUtils.concatPath(inodePath.getUri(), childStatus.getName()));
            if (mInodeTree.inodePathExists(childURI) && (childStatus.isFile()
                || options.getLoadDescendantType() != DescendantType.ALL)) {
              // stop traversing if this is an existing file, or an existing directory without
              // loading all descendants.
              continue;
            }

            try (LockedInodePath tempInodePath =
                inodePath.createTempPathForChild(childStatus.getName())) {
              LoadMetadataOptions loadMetadataOptions =
                  LoadMetadataOptions.defaults().setLoadDescendantType(DescendantType.NONE)
                      .setCreateAncestors(false).setUfsStatus(childStatus);
              loadMetadataAndJournal(rpcContext, tempInodePath, loadMetadataOptions);

              if (options.getLoadDescendantType() == DescendantType.ALL
                  && tempInodePath.getInode().isDirectory()) {
                InodeDirectory inodeDirectory = (InodeDirectory) tempInodePath.getInode();
                inodeDirectory.setDirectChildrenLoaded(true);
              }
            }

          }
          inode.setDirectChildrenLoaded(true);
        }
      }
    } catch (IOException e) {
      LOG.debug("Failed to loadMetadataAndJournal: inodePath={}, options={}.", inodePath.getUri(),
          options, e);
      throw e;
    }
  }

  /**
   * Loads metadata for the file identified by the given path from UFS into Alluxio.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param resolution the UFS resolution of path
   * @param options the load metadata options
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws AccessControlException if permission checking fails or permission setting fails
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if invalid file size is encountered
   */
  private void loadFileMetadataAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      FileAlreadyCompletedException, InvalidFileSizeException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    AlluxioURI ufsUri = resolution.getUri();
    UfsFileStatus ufsStatus = (UfsFileStatus) options.getUfsStatus();
    long ufsBlockSizeByte;
    long ufsLength;
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();

      ufsBlockSizeByte = ufs.getBlockSizeByte(ufsUri.toString());
      if (ufsStatus == null) {
        ufsStatus = ufs.getFileStatus(ufsUri.toString());
      }
      ufsLength = ufsStatus.getContentLength();
    }

    // Metadata loaded from UFS has no TTL set.
    CreateFileOptions createFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(ufsBlockSizeByte)
            .setRecursive(options.isCreateAncestors()).setMetadataLoad(true).setPersisted(true);
    String ufsOwner = ufsStatus.getOwner();
    String ufsGroup = ufsStatus.getGroup();
    short ufsMode = ufsStatus.getMode();
    Mode mode = new Mode(ufsMode);
    Long ufsLastModified = ufsStatus.getLastModifiedTime();
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileOptions = createFileOptions.setOwner(ufsOwner).setGroup(ufsGroup).setMode(mode);
    if (ufsLastModified != null) {
      createFileOptions.setOperationTimeMs(ufsLastModified);
    }

    try {
      createFileAndJournal(rpcContext, inodePath, createFileOptions);
      CompleteFileOptions completeOptions = CompleteFileOptions.defaults().setUfsLength(ufsLength)
          .setUfsStatus(ufsStatus);
      if (ufsLastModified != null) {
        completeOptions.setOperationTimeMs(ufsLastModified);
      }
      completeFileAndJournal(rpcContext, inodePath, completeOptions);
      if (inodePath.getLockMode() == InodeTree.LockMode.READ) {
        // After completing the inode, the lock on the last inode which stands for the created file
        // should be downgraded to a read lock, so that it won't block the reads operations from
        // other thread. More importantly, it's possible the subsequent read operations within the
        // same thread may the read parent nodes along the path, and multiple similar threads may
        // lock each other. For example, getFileStatus will discover the metadata of UFS files and
        // it creates an inode per discovered. Concurrent getFileStatus of the same directory will
        // lead to such contention.
        inodePath.downgradeLast();
      }
    } catch (FileAlreadyExistsException e) {
      // This may occur if there are concurrent load metadata requests. To allow loading metadata
      // to be idempotent, ensure the full path exists when this happens.
      mInodeTree.ensureFullInodePath(inodePath, inodePath.getLockMode());
    }
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   * It is a no-op if the directory exists and is persisted.
   *
   * @param rpcContext the rpc context
   * @param inodePath the path for which metadata should be loaded
   * @param options the load metadata options
   * @throws InvalidPathException if invalid path is encountered
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */

  private void loadDirectoryMetadataAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    if (inodePath.fullPathExists()) {
      if (inodePath.getInode().isPersisted()) {
        return;
      }
    }
    CreateDirectoryOptions createDirectoryOptions = CreateDirectoryOptions.defaults()
        .setMountPoint(mMountTable.isMountPoint(inodePath.getUri())).setPersisted(true)
        .setRecursive(options.isCreateAncestors()).setMetadataLoad(true).setAllowExists(true);
    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    UfsStatus ufsStatus = options.getUfsStatus();
    if (ufsStatus == null) {
      AlluxioURI ufsUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        ufsStatus = ufs.getDirectoryStatus(ufsUri.toString());
      }
    }
    String ufsOwner = ufsStatus.getOwner();
    String ufsGroup = ufsStatus.getGroup();
    short ufsMode = ufsStatus.getMode();
    Long lastModifiedTime = ufsStatus.getLastModifiedTime();
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryOptions = createDirectoryOptions.setOwner(ufsOwner).setGroup(ufsGroup)
            .setMode(mode).setUfsStatus(ufsStatus);
    if (lastModifiedTime != null) {
      createDirectoryOptions.setOperationTimeMs(lastModifiedTime);
    }

    try {
      createDirectoryAndJournal(rpcContext, inodePath, createDirectoryOptions);
      if (inodePath.getLockMode() == InodeTree.LockMode.READ) {
        // If the directory is successfully created, createDirectoryAndJournal will add a write lock
        // to the inodePath's lock list. We are done modifying the directory, so we downgrade it to
        // a read lock.
        inodePath.downgradeLast();
      }
    } catch (FileAlreadyExistsException e) {
      // This may occur if there are concurrent load metadata requests. To allow loading metadata
      // to be idempotent, ensure the full path exists when this happens.
      mInodeTree.ensureFullInodePath(inodePath, inodePath.getLockMode());
    }
  }

  /**
   * Loads metadata for the path if it is (non-existing || load direct children is set).
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to load the metadata for
   * @param options the load metadata options
   */
  private void loadMetadataIfNotExistAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataOptions options) {
    boolean inodeExists = inodePath.fullPathExists();
    boolean loadDirectChildren = false;
    if (inodeExists) {
      try {
        Inode<?> inode = inodePath.getInode();
        loadDirectChildren =
            inode.isDirectory() && (options.getLoadDescendantType() != DescendantType.NONE);
      } catch (FileDoesNotExistException e) {
        // This should never happen.
        throw new RuntimeException(e);
      }
    }
    if (!inodeExists || loadDirectChildren) {
      try {
        loadMetadataAndJournal(rpcContext, inodePath, options);
      } catch (Exception e) {
        // NOTE, this may be expected when client tries to get info (e.g. exists()) for a file
        // existing neither in Alluxio nor UFS.
        LOG.debug("Failed to load metadata for path from UFS: {}", inodePath.getUri());
      }
    }
  }

  @Override
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.MOUNT_OPS.inc();
    LockingScheme lockingScheme =
        createLockingScheme(alluxioPath, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
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

      mountAndJournal(rpcContext, inodePath, ufsPath, options);
      auditContext.setSucceeded(true);
      Metrics.PATHS_MOUNTED.inc();
    }
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws AccessControlException if the permission check fails
   */
  private void mountAndJournal(RpcContext rpcContext, LockedInodePath inodePath, AlluxioURI ufsPath,
      MountOptions options) throws InvalidPathException, FileAlreadyExistsException,
      FileDoesNotExistException, IOException, AccessControlException {
    // Check that the Alluxio Path does not exist
    if (inodePath.fullPathExists()) {
      // TODO(calvin): Add a test to validate this (ALLUXIO-1831)
      throw new InvalidPathException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(inodePath.getUri()));
    }
    long mountId = IdUtils.createMountId();
    mountInternal(inodePath, ufsPath, mountId, false /* not replayed */, options);
    boolean loadMetadataSucceeded = false;
    try {
      // This will create the directory at alluxioPath
      loadDirectoryMetadataAndJournal(rpcContext, inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(false));
      loadMetadataSucceeded = true;
    } finally {
      if (!loadMetadataSucceeded) {
        unmountInternal(inodePath.getUri());
      }
    }

    // For proto, build a list of String pairs representing the properties map.
    Map<String, String> properties = options.getProperties();
    List<StringPairEntry> protoProperties = new ArrayList<>(properties.size());
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      protoProperties.add(
          StringPairEntry.newBuilder().setKey(entry.getKey()).setValue(entry.getValue()).build());
    }

    AddMountPointEntry addMountPoint =
        AddMountPointEntry.newBuilder().setAlluxioPath(inodePath.getUri().toString())
            .setUfsPath(ufsPath.toString()).setMountId(mountId)
            .setReadOnly(options.isReadOnly())
            .addAllProperties(protoProperties).setShared(options.isShared()).build();
    rpcContext.journal(JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build());
  }

  /**
   * @param entry the entry to use
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void mountFromEntry(AddMountPointEntry entry)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    AlluxioURI ufsURI = new AlluxioURI(entry.getUfsPath());
    try (LockedInodePath inodePath = mInodeTree
        .lockInodePath(alluxioURI, InodeTree.LockMode.WRITE)) {
      mountInternal(inodePath, ufsURI, entry.getMountId(), true /* replayed */,
          new MountOptions(entry));
    }
  }

  /**
   * Updates the mount table with the specified mount point. The mount options may be updated during
   * this method.
   *
   * @param inodePath the Alluxio mount point
   * @param ufsPath the UFS endpoint to mount
   * @param mountId the mount id
   * @param replayed whether the operation is a result of replaying the journal
   * @param options the mount options (may be updated)
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void mountInternal(LockedInodePath inodePath, AlluxioURI ufsPath, long mountId,
      boolean replayed, MountOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioPath = inodePath.getUri();
    // Adding the mount point will not create the UFS instance and thus not connect to UFS
    mUfsManager.addMount(mountId, new AlluxioURI(ufsPath.toString()),
        UnderFileSystemConfiguration.defaults().setReadOnly(options.isReadOnly())
            .setShared(options.isShared()).setUserSpecifiedConf(options.getProperties()));
    try {
      if (!replayed) {
        try (CloseableResource<UnderFileSystem> ufsResource =
            mUfsManager.get(mountId).acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          ufs.connectFromMaster(
              NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC));
          // Check that the ufsPath exists and is a directory
          if (!ufs.isDirectory(ufsPath.toString())) {
            throw new IOException(
                ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
          }
        }
        // Check that the alluxioPath we're creating doesn't shadow a path in the default UFS
        String defaultUfsPath = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
        UnderFileSystem defaultUfs = UnderFileSystem.Factory.createForRoot();
        String shadowPath = PathUtils.concatPath(defaultUfsPath, alluxioPath.getPath());
        if (defaultUfs.exists(shadowPath)) {
          throw new IOException(
              ExceptionMessage.MOUNT_PATH_SHADOWS_DEFAULT_UFS.getMessage(alluxioPath));
        }
      }

      // Add the mount point. This will only succeed if we are not mounting a prefix of an existing
      // mount and no existing mount is a prefix of this mount.
      mMountTable.add(alluxioPath, ufsPath, mountId, options);
    } catch (Exception e) {
      mUfsManager.removeMount(mountId);
      throw e;
    }
  }

  @Override
  public void unmount(AlluxioURI alluxioPath) throws FileDoesNotExistException,
      InvalidPathException, IOException, AccessControlException {
    Metrics.UNMOUNT_OPS.inc();
    List<Inode<?>> deletedInodes;
    // Unmount should lock the parent to remove the child inode.
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(alluxioPath, InodeTree.LockMode.WRITE_PARENT);
         FileSystemMasterAuditContext auditContext =
             createAuditContext("unmount", alluxioPath, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      unmountAndJournal(rpcContext, inodePath);
      auditContext.setSucceeded(true);
      Metrics.PATHS_UNMOUNTED.inc();
    }
  }

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * This method does not delete blocks. Instead, it adds the to the passed-in block deletion
   * context so that the blocks can be deleted after the inode deletion journal entry has been
   * written. We cannot delete blocks earlier because the inode deletion may fail, leaving us with
   * inode containing deleted blocks.
   *
   * @param rpcContext the rpc context
   * @param inodePath the Alluxio path to unmount, must be a mount point
   * @throws InvalidPathException if the given path is not a mount point
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   */
  private void unmountAndJournal(RpcContext rpcContext, LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException, IOException {
    if (!unmountInternal(inodePath.getUri())) {
      throw new InvalidPathException("Failed to unmount " + inodePath.getUri() + ". Please ensure"
          + " the path is an existing mount point and not root.");
    }
    try {
      // Use the internal delete API, setting {@code alluxioOnly} to true to prevent the delete
      // operations from being persisted in the UFS.
      DeleteOptions deleteOptions =
          DeleteOptions.defaults().setRecursive(true).setAlluxioOnly(true);
      deleteAndJournal(rpcContext, inodePath, deleteOptions);
    } catch (DirectoryNotEmptyException e) {
      throw new RuntimeException(String.format(
          "We should never see this exception because %s should never be thrown when recursive "
              + "is true.",
          e.getClass()));
    }
    DeleteMountPointEntry deleteMountPoint =
        DeleteMountPointEntry.newBuilder().setAlluxioPath(inodePath.getUri().toString()).build();
    rpcContext.journal(JournalEntry.newBuilder().setDeleteMountPoint(deleteMountPoint).build());
  }

  /**
   * @param entry the entry to use
   */
  private void unmountFromEntry(DeleteMountPointEntry entry) {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    if (!unmountInternal(alluxioURI)) {
      LOG.error("Failed to unmount {}", alluxioURI);
    }
  }

  /**
   * @param uri the Alluxio mount point to remove from the mount table
   * @return true if successful, false otherwise
   */
  private boolean unmountInternal(AlluxioURI uri) {
    return mMountTable.delete(uri);
  }

  @Override
  public void resetFile(long fileId)
      throws UnexpectedAlluxioException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException {
    // TODO(yupeng) check the file is not persisted
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
      // free the file first
      InodeFile inodeFile = inodePath.getInodeFile();
      freeAndJournal(rpcContext, inodePath, FreeOptions.defaults().setForced(true));
      inodeFile.reset();
    }
  }

  @Override
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException,
      IOException {
    Metrics.SET_ATTRIBUTE_OPS.inc();
    // for chown
    boolean rootRequired = options.getOwner() != null;
    // for chgrp, chmod
    boolean ownerRequired =
        (options.getGroup() != null) || (options.getMode() != Constants.INVALID_MODE);
    if (options.getOwner() != null && options.getGroup() != null) {
      try {
        checkUserBelongsToGroup(options.getOwner(), options.getGroup());
      } catch (IOException e) {
        throw new IOException(String.format("Could not update owner:group for %s to %s:%s. %s",
            path.toString(), options.getOwner(), options.getGroup(), e.toString()), e);
      }
    }
    String commandName;
    if (options.getOwner() != null) {
      commandName = "chown";
    } else if (options.getGroup() != null) {
      commandName = "chgrp";
    } else if (options.getMode() != null) {
      commandName = "chmod";
    } else {
      commandName = "setAttribute";
    }
    LockingScheme lockingScheme =
        createLockingScheme(path, options.getCommonOptions(), InodeTree.LockMode.WRITE);
    try (RpcContext rpcContext = createRpcContext();
         LockedInodePath inodePath = mInodeTree
             .lockInodePath(lockingScheme.getPath(), lockingScheme.getMode());
         FileSystemMasterAuditContext auditContext =
             createAuditContext(commandName, path, null, inodePath.getInodeOrNull())) {
      try {
        mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
      } catch (AccessControlException e) {
        auditContext.setAllowed(false);
        throw e;
      }
      mMountTable.checkUnderWritableMountPoint(path);
      // Possible ufs sync.
      syncMetadata(rpcContext, inodePath, lockingScheme,
          options.isRecursive() ? DescendantType.ALL : DescendantType.ONE);
      if (!inodePath.fullPathExists()) {
        throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(path));
      }

      setAttributeAndJournal(rpcContext, inodePath, rootRequired, ownerRequired, options);
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
    List<String> groups = CommonUtils.getGroups(owner);
    if (groups == null || !groups.contains(group)) {
      throw new FailedPreconditionException("Owner " + owner
          + " does not belong to the group " + group);
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} to set attribute for
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws InvalidPathException if the given path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   */
  private void setAttributeAndJournal(RpcContext rpcContext, LockedInodePath inodePath,
      boolean rootRequired, boolean ownerRequired, SetAttributeOptions options)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException, IOException {
    Inode<?> targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (options.isRecursive() && targetInode.isDirectory()) {
      try (LockedInodePathList descendants = mInodeTree.lockDescendants(inodePath,
          InodeTree.LockMode.WRITE)) {
        for (LockedInodePath childPath : descendants.getInodePathList()) {
          mPermissionChecker.checkSetAttributePermission(childPath, rootRequired, ownerRequired);
        }
        for (LockedInodePath childPath : descendants.getInodePathList()) {
          List<Inode<?>> persistedInodes =
              setAttributeInternal(childPath, false, opTimeMs, options);
          journalPersistedInodes(persistedInodes, rpcContext.getJournalContext());
          journalSetAttribute(childPath, opTimeMs, options, rpcContext.getJournalContext());
        }
      }
    }
    List<Inode<?>> persistedInodes = setAttributeInternal(inodePath, false, opTimeMs, options);
    journalPersistedInodes(persistedInodes, rpcContext.getJournalContext());
    journalSetAttribute(inodePath, opTimeMs, options, rpcContext.getJournalContext());
  }

  /**
   * @param inodePath the file path to use
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @param journalContext the journal context
   * @throws FileDoesNotExistException if path does not exist
   */
  private void journalSetAttribute(LockedInodePath inodePath, long opTimeMs,
      SetAttributeOptions options, JournalContext journalContext) throws FileDoesNotExistException {
    SetAttributeEntry.Builder builder =
        SetAttributeEntry.newBuilder().setId(inodePath.getInode().getId()).setOpTimeMs(opTimeMs);
    if (options.getPinned() != null) {
      builder.setPinned(options.getPinned());
    }
    if (options.getTtl() != null) {
      builder.setTtl(options.getTtl());
      builder.setTtlAction(ProtobufUtils.toProtobuf(options.getTtlAction()));
    }

    if (options.getPersisted() != null) {
      builder.setPersisted(options.getPersisted());
    }
    if (options.getOwner() != null) {
      builder.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      builder.setGroup(options.getGroup());
    }
    if (options.getMode() != Constants.INVALID_MODE) {
      builder.setPermission(options.getMode());
    }
    if (!options.getUfsFingerprint().equals(Constants.INVALID_UFS_FINGERPRINT)) {
      builder.setUfsFingerprint(options.getUfsFingerprint());
    }
    journalContext.append(JournalEntry.newBuilder().setSetAttribute(builder).build());
  }

  @Override
  public void scheduleAsyncPersistence(AlluxioURI path)
      throws AlluxioException, UnavailableException {
    checkUfsMode(path, OperationType.WRITE);
    try (RpcContext rpcContext = createRpcContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      scheduleAsyncPersistenceAndJournal(rpcContext, inodePath);
    }
    // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
    mAsyncPersistHandler.scheduleAsyncPersistence(path);
  }

  /**
   * Schedules a file for async persistence.
   * <p>
   * Writes to the journal.
   *
   * @param rpcContext the rpc context
   * @param inodePath the {@link LockedInodePath} of the file for persistence
   */
  private void scheduleAsyncPersistenceAndJournal(RpcContext rpcContext, LockedInodePath inodePath)
      throws AlluxioException {
    long fileId = inodePath.getInode().getId();
    scheduleAsyncPersistenceInternal(inodePath);
    // write to journal
    AsyncPersistRequestEntry asyncPersistRequestEntry =
        AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
    rpcContext.journal(
        JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build());
  }

  /**
   * @param inodePath the {@link LockedInodePath} of the file to persist
   */
  private void scheduleAsyncPersistenceInternal(LockedInodePath inodePath) throws AlluxioException {
    inodePath.getInode().setPersistenceState(PersistenceState.TO_BE_PERSISTED);
  }

  /**
   * Syncs the Alluxio metadata with UFS.
   *
   * @param rpcContext the rpcContext
   * @param inodePath the Alluxio inode path to sync with UFS
   * @param lockingScheme the locking scheme used to lock the inode path
   * @param syncDescendantType how to sync descendants
   * @return true if the sync was performed successfully, false otherwise (including errors)
   */
  private boolean syncMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LockingScheme lockingScheme, DescendantType syncDescendantType) {
    if (!lockingScheme.shouldSync()) {
      return false;
    }

    // The high-level process for the syncing is:
    // 1. Find all Alluxio paths which are not consistent with the corresponding UFS path.
    //    This means the UFS path does not exist, or is different from the Alluxio metadata.
    // 2. If only the metadata changed for a file or a directory,, update the inode with
    //    new metadata from the UFS.
    // 3. Delete any Alluxio path whose content is not consistent with UFS, or not in UFS. After
    //    this step, all the paths in Alluxio are consistent with UFS, and there may be additional
    //    UFS paths to load.
    // 4. Load metadata from UFS.

    Set<String> pathsToLoad = new HashSet<>();

    // Set to true if the given inode was deleted.
    boolean deletedInode = false;

    try {
      if (!inodePath.fullPathExists()) {
        // The requested path does not exist in Alluxio, so just load metadata.
        pathsToLoad.add(inodePath.getUri().getPath());
      } else {
        SyncResult result =
            syncInodeMetadata(rpcContext, inodePath, syncDescendantType);
        deletedInode = result.getDeletedInode();
        pathsToLoad = result.getPathsToLoad();
      }
    } catch (Exception e) {
      LOG.error("Failed to remove out-of-sync metadata for path: {}", inodePath.getUri(), e);
      return false;
    } finally {
      if (deletedInode) {
        // If the inode was deleted, then the inode path should reflect the delete.
        inodePath.unlockLast();
      } else {
        // If the inode was not deleted, downgrade the last inode lock, if necessary.
        inodePath.downgradeLastWithScheme(lockingScheme);
      }
    }

    // Update metadata for all the mount points
    for (String mountPoint : pathsToLoad) {
      AlluxioURI mountPointUri = new AlluxioURI(mountPoint);
      try {
        if (PathUtils.hasPrefix(inodePath.getUri().getPath(), mountPointUri.getPath())) {
          // one of the mountpoint is above the original inodePath, we start loading from the
          // original inodePath. It is already locked. so we proceed to load metadata.
          try {
            loadMetadataAndJournal(rpcContext, inodePath, LoadMetadataOptions.defaults()
                .setCreateAncestors(true).setLoadDescendantType(syncDescendantType));

            mUfsSyncPathCache.notifySyncedPath(inodePath.getUri().getPath());
          } catch (Exception e) {
            // This may be expected. For example, when creating a new file, the UFS file is not
            // expected to exist.
            LOG.debug("Failed to load metadata for path: {}", inodePath.getUri(), e);
            continue;
          }
        } else {
          try (LockedInodePath descendantPath =
                   mInodeTree.lockDescendantPath(inodePath, lockingScheme.getMode(),
                       mountPointUri)) {
            try {
              loadMetadataAndJournal(rpcContext, descendantPath, LoadMetadataOptions.defaults()
                  .setCreateAncestors(true).setLoadDescendantType(syncDescendantType));
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
    return true;
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
   * @return the result of the sync, including if the inode was deleted, and if further load
   *         metadata is required
   */
  private SyncResult syncInodeMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      DescendantType syncDescendantType)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // Set of paths to sync
    Set<String> pathsToLoad = new HashSet<>();

    // The options for deleting.
    DeleteOptions syncDeleteOptions =
        DeleteOptions.defaults().setRecursive(true).setAlluxioOnly(true).setUnchecked(true);

    // The requested path already exists in Alluxio.
    Inode<?> inode = inodePath.getInode();

    if (inode instanceof InodeFile && !((InodeFile) inode).isCompleted()) {
      // Do not sync an incomplete file, since the UFS file is expected to not exist.
      return SyncResult.defaults();
    }
    if (inode.getPersistenceState() == PersistenceState.TO_BE_PERSISTED) {
      // Do not sync a file in the process of being persisted, since the UFS file is being
      // written.
      return SyncResult.defaults();
    }

    MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      String ufsFingerprint = ufs.getFingerprint(ufsUri.toString());
      Fingerprint ufsFpParsed = Fingerprint.parse(ufsFingerprint);
      boolean containsMountPoint = mMountTable.containsMountPoint(inodePath.getUri());

      UfsSyncUtils.SyncPlan syncPlan =
          UfsSyncUtils.computeSyncPlan(inode, ufsFpParsed, containsMountPoint);

      if (syncPlan.toUpdateMetaData()) {
        // UpdateMetadata is used when a file or a directory only had metadata change.
        // It works by calling SetAttributeInternal on the inodePath.
        if (ufsFpParsed.isValid()) {
          short mode = Short.parseShort(ufsFpParsed.getTag(Tag.MODE));
          SetAttributeOptions options =
              SetAttributeOptions.defaults().setOwner(ufsFpParsed.getTag(Tag.OWNER))
                  .setGroup(ufsFpParsed.getTag(Tag.GROUP))
                  .setMode(mode)
                  .setUfsFingerprint(ufsFingerprint);
          long opTimeMs = System.currentTimeMillis();
          // use replayed, since updating UFS is not desired.
          setAttributeInternal(inodePath, true, opTimeMs, options);
          journalSetAttribute(inodePath, opTimeMs, options, rpcContext.getJournalContext());
        }
      }
      if (syncPlan.toDelete()) {
        try {
          deleteInternal(rpcContext, inodePath, false, System.currentTimeMillis(),
              syncDeleteOptions);

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
      if (syncPlan.toSyncChildren() && inode instanceof InodeDirectory
          && syncDescendantType != DescendantType.NONE) {
        InodeDirectory inodeDir = (InodeDirectory) inode;
        // maps children name to inode
        Map<String, Inode<?>> inodeChildren = new HashMap<>();
        for (Inode<?> child : inodeDir.getChildren()) {
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
        for (Map.Entry<String, Inode<?>> inodeEntry : inodeChildren.entrySet()) {
          if (!inodeEntry.getValue().isPersisted()) {
            // Ignore non-persisted inodes.
            continue;
          }
          try (LockedInodePath tempInodePath = mInodeTree.lockDescendantPath(inodePath,
              InodeTree.LockMode.READ, inodePath.getUri().join(inodeEntry.getKey()))) {
            // Recursively sync children
            if (syncDescendantType != DescendantType.ALL) {
              syncDescendantType = DescendantType.NONE;
            }
            SyncResult syncResult =
                syncInodeMetadata(rpcContext, tempInodePath, syncDescendantType);
            pathsToLoad.addAll(syncResult.getPathsToLoad());
          }
        }
      }
    }
    return new SyncResult(deletedInode, pathsToLoad);
  }

  @Override
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles,
      WorkerHeartbeatOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      IOException {

    List<String> persistedUfsFingerprints = options.getPersistedUfsFingerprintList();
    boolean hasPersistedFingerprints = persistedUfsFingerprints.size() == persistedFiles.size();
    for (int i = 0; i < persistedFiles.size(); i++) {
      long fileId = persistedFiles.get(i);
      String ufsFingerprint = hasPersistedFingerprints ? persistedUfsFingerprints.get(i) :
          Constants.INVALID_UFS_FINGERPRINT;
      try {
        // Permission checking for each file is performed inside setAttribute
        setAttribute(getPath(fileId),
            SetAttributeOptions.defaults().setPersisted(true).setUfsFingerprint(ufsFingerprint));
      } catch (FileDoesNotExistException | AccessControlException | InvalidPathException e) {
        LOG.error("Failed to set file {} as persisted, because {}", fileId, e);
      }
    }

    // get the files for the given worker to persist
    List<PersistFile> filesToPersist = mAsyncPersistHandler.pollFilesToPersist(workerId);
    if (!filesToPersist.isEmpty()) {
      LOG.debug("Sent files {} to worker {} to persist", filesToPersist, workerId);
    }
    FileSystemCommandOptions commandOptions = new FileSystemCommandOptions();
    commandOptions.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, commandOptions);
  }

  /**
   * @param inodePath the {@link LockedInodePath} to use
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the operation time (in milliseconds)
   * @param options the method options
   * @return list of inodes which were marked as persisted
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private List<Inode<?>> setAttributeInternal(LockedInodePath inodePath, boolean replayed,
      long opTimeMs, SetAttributeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<Inode<?>> persistedInodes = Collections.emptyList();
    Inode<?> inode = inodePath.getInode();
    if (options.getPinned() != null) {
      mInodeTree.setPinned(inodePath, options.getPinned(), opTimeMs);
      inode.setLastModificationTimeMs(opTimeMs);
    }
    if (options.getTtl() != null) {
      long ttl = options.getTtl();
      if (inode.getTtl() != ttl || inode.getTtlAction() != options.getTtlAction()) {
        if (inode.getTtl() != ttl) {
          mTtlBuckets.remove(inode);
          inode.setTtl(ttl);
          mTtlBuckets.insert(inode);
        }
        inode.setLastModificationTimeMs(opTimeMs);
        inode.setTtlAction(options.getTtlAction());
      }
    }
    if (options.getPersisted() != null) {
      Preconditions.checkArgument(inode.isFile(), PreconditionMessage.PERSIST_ONLY_FOR_FILE);
      Preconditions.checkArgument(((InodeFile) inode).isCompleted(),
          PreconditionMessage.FILE_TO_PERSIST_MUST_BE_COMPLETE);
      InodeFile file = (InodeFile) inode;
      // TODO(manugoyal) figure out valid behavior in the un-persist case
      Preconditions
          .checkArgument(options.getPersisted(), PreconditionMessage.ERR_SET_STATE_UNPERSIST);
      if (!file.isPersisted()) {
        file.setPersistenceState(PersistenceState.PERSISTED);
        persistedInodes = propagatePersistedInternal(inodePath, false);
        file.setLastModificationTimeMs(opTimeMs);
        Metrics.FILES_PERSISTED.inc();
      }
    }
    boolean ownerGroupChanged = (options.getOwner() != null) || (options.getGroup() != null);
    boolean modeChanged = (options.getMode() != Constants.INVALID_MODE);
    // If the file is persisted in UFS, also update corresponding owner/group/permission.
    if ((ownerGroupChanged || modeChanged) && !replayed && inode.isPersisted()) {
      if ((inode instanceof InodeFile) && !((InodeFile) inode).isCompleted()) {
        LOG.debug("Alluxio does not propagate chown/chgrp/chmod to UFS for incomplete files.");
      } else {
        checkUfsMode(inodePath.getUri(), OperationType.WRITE);
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        String ufsUri = resolution.getUri().toString();
        try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
          UnderFileSystem ufs = ufsResource.get();
          if (ufs.isObjectStorage()) {
            LOG.warn("setOwner/setMode is not supported to object storage UFS via Alluxio. "
                + "UFS: " + ufsUri + ". This has no effect on the underlying object.");
          } else {
            String owner = null;
            String group = null;
            String mode = null;
            if (ownerGroupChanged) {
              try {
                owner = options.getOwner() != null ? options.getOwner() : inode.getOwner();
                group = options.getGroup() != null ? options.getGroup() : inode.getGroup();
                ufs.setOwner(ufsUri, owner, group);
              } catch (IOException e) {
                throw new AccessControlException("Could not setOwner for UFS file " + ufsUri
                    + " . Aborting the setAttribute operation in Alluxio.", e);
              }
            }
            if (modeChanged) {
              try {
                mode = String.valueOf(options.getMode());
                ufs.setMode(ufsUri, options.getMode());
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
              options.setUfsFingerprint(fp.serialize());
            } else {
              // Need to retrieve the fingerprint from ufs.
              options.setUfsFingerprint(ufs.getFingerprint(ufsUri));
            }
          }
        }
      }
    }
    if (!options.getUfsFingerprint().equals(Constants.INVALID_UFS_FINGERPRINT)) {
      inode.setUfsFingerprint(options.getUfsFingerprint());
    }
    // Only commit the set permission to inode after the propagation to UFS succeeded.
    if (options.getOwner() != null) {
      inode.setOwner(options.getOwner());
    }
    if (options.getGroup() != null) {
      inode.setGroup(options.getGroup());
    }
    if (modeChanged) {
      inode.setMode(options.getMode());
    }
    return persistedInodes;
  }

  /**
   * @param entry the entry to use
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if failed to set permission
   */
  private void setAttributeFromEntry(SetAttributeEntry entry)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    SetAttributeOptions options = SetAttributeOptions.defaults();
    if (entry.hasPinned()) {
      options.setPinned(entry.getPinned());
    }
    if (entry.hasTtl()) {
      options.setTtl(entry.getTtl());
      options.setTtlAction(ProtobufUtils.fromProtobuf(entry.getTtlAction()));
    }
    if (entry.hasPersisted()) {
      options.setPersisted(entry.getPersisted());
    }
    if (entry.hasOwner()) {
      options.setOwner(entry.getOwner());
    }
    if (entry.hasGroup()) {
      options.setGroup(entry.getGroup());
    }
    if (entry.hasPermission()) {
      options.setMode((short) entry.getPermission());
    }
    if (entry.hasUfsFingerprint()) {
      options.setUfsFingerprint(entry.getUfsFingerprint());
    }
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      setAttributeInternal(inodePath, true, entry.getOpTimeMs(), options);
      // Intentionally not journaling the persisted inodes from setAttributeInternal
    }
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    return mBlockMaster.getWorkerInfoList();
  }

  @Override
  public void updateUfsMode(AlluxioURI ufsPath, UnderFileSystem.UfsMode ufsMode)
      throws InvalidPathException, InvalidArgumentException, UnavailableException,
      AccessControlException {
    // TODO(adit): Create new fsadmin audit context
    try (RpcContext rpcContext = createRpcContext();
        FileSystemMasterAuditContext auditContext =
            createAuditContext("updateUfsMode", ufsPath, null, null)) {
      updateUfsModeAndJournal(rpcContext, ufsPath, ufsMode);
      auditContext.setSucceeded(true);
    }
  }

  /**
   * Update the ufs mode for path and create a journal entry.
   *
   * @param rpcContext the rpc context
   * @param ufsPath the ufs path
   * @param ufsMode the ufs mode
   * @throws InvalidPathException if path is not used by any mount point
   * @throws InvalidArgumentException if arguments for the method are invalid
   */
  private void updateUfsModeAndJournal(RpcContext rpcContext, AlluxioURI ufsPath,
      UnderFileSystem.UfsMode ufsMode) throws InvalidPathException, InvalidArgumentException {
    updateUfsModeInternal(ufsPath, ufsMode);
    // Journal
    File.UfsMode ufsModeEntry;
    switch (ufsMode) {
      case NO_ACCESS:
        ufsModeEntry = File.UfsMode.NO_ACCESS;
        break;
      case READ_ONLY:
        ufsModeEntry = File.UfsMode.READ_ONLY;
        break;
      case READ_WRITE:
        ufsModeEntry = File.UfsMode.READ_WRITE;
        break;
      default:
        throw new InvalidArgumentException(ExceptionMessage.INVALID_UFS_MODE.getMessage(ufsMode));
    }
    File.UpdateUfsModeEntry ufsEntry = File.UpdateUfsModeEntry.newBuilder()
        .setUfsPath(ufsPath.getRootPath()).setUfsMode(ufsModeEntry).build();
    rpcContext.journal(JournalEntry.newBuilder().setUpdateUfsMode(ufsEntry).build());
  }

  /**
   * Update ufs mode from journal entry.
   *
   * @param entry the update ufs mode journal entry
   * @throws InvalidPathException if the path is not used by any mount point
   * @throws InvalidArgumentException if arguments for the method are invalid
   */
  private void updateUfsModeFromEntry(File.UpdateUfsModeEntry entry)
      throws InvalidPathException, InvalidArgumentException {
    String ufsPath = entry.getUfsPath();
    UnderFileSystem.UfsMode ufsMode;
    switch (entry.getUfsMode()) {
      case NO_ACCESS:
        ufsMode = UnderFileSystem.UfsMode.NO_ACCESS;
        break;
      case READ_ONLY:
        ufsMode = UnderFileSystem.UfsMode.READ_ONLY;
        break;
      case READ_WRITE:
        ufsMode = UnderFileSystem.UfsMode.READ_WRITE;
        break;
      default:
        throw new InvalidArgumentException(
            ExceptionMessage.INVALID_UFS_MODE.getMessage(entry.getUfsMode()));
    }
    updateUfsModeInternal(new AlluxioURI(ufsPath), ufsMode);
  }

  private void updateUfsModeInternal(AlluxioURI ufsPath, UnderFileSystem.UfsMode ufsMode)
      throws InvalidPathException {
    mUfsManager.setUfsMode(ufsPath, ufsMode);
  }

  /**
   * Check if the specified operation type is allowed to the ufs.
   *
   * @param alluxioPath the Alluxio path
   * @param opType the operation type
   * @throws AccessControlException if the specified operation is not allowed
   */
  private void checkUfsMode(AlluxioURI alluxioPath, OperationType opType)
      throws AccessControlException, InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioPath);
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      UnderFileSystem.UfsMode ufsMode =
          ufs.getOperationMode(mUfsManager.getPhysicalUfsState(ufs.getPhysicalStores()));
      switch (ufsMode) {
        case NO_ACCESS:
          throw new AccessControlException(ExceptionMessage.UFS_OP_NOT_ALLOWED.getMessage(opType,
              resolution.getUri(), UnderFileSystem.UfsMode.NO_ACCESS));
        case READ_ONLY:
          if (opType == OperationType.WRITE) {
            throw new AccessControlException(ExceptionMessage.UFS_OP_NOT_ALLOWED.getMessage(opType,
                resolution.getUri(), UnderFileSystem.UfsMode.READ_ONLY));
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
   * {@link alluxio.web.WebInterfaceMasterMetricsServlet}.
   */
  public static final class Metrics {
    private static final Counter DIRECTORIES_CREATED = MetricsSystem.counter("DirectoriesCreated");
    private static final Counter FILE_BLOCK_INFOS_GOT = MetricsSystem.counter("FileBlockInfosGot");
    private static final Counter FILE_INFOS_GOT = MetricsSystem.counter("FileInfosGot");
    private static final Counter FILES_COMPLETED = MetricsSystem.counter("FilesCompleted");
    private static final Counter FILES_CREATED = MetricsSystem.counter("FilesCreated");
    private static final Counter FILES_FREED = MetricsSystem.counter("FilesFreed");
    private static final Counter FILES_PERSISTED = MetricsSystem.counter("FilesPersisted");
    private static final Counter NEW_BLOCKS_GOT = MetricsSystem.counter("NewBlocksGot");
    private static final Counter PATHS_DELETED = MetricsSystem.counter("PathsDeleted");
    private static final Counter PATHS_MOUNTED = MetricsSystem.counter("PathsMounted");
    private static final Counter PATHS_RENAMED = MetricsSystem.counter("PathsRenamed");
    private static final Counter PATHS_UNMOUNTED = MetricsSystem.counter("PathsUnmounted");

    // TODO(peis): Increment the RPCs OPs at the place where we receive the RPCs.
    private static final Counter COMPLETE_FILE_OPS = MetricsSystem.counter("CompleteFileOps");
    private static final Counter CREATE_DIRECTORIES_OPS =
        MetricsSystem.counter("CreateDirectoryOps");
    private static final Counter CREATE_FILES_OPS = MetricsSystem.counter("CreateFileOps");
    private static final Counter DELETE_PATHS_OPS = MetricsSystem.counter("DeletePathOps");
    private static final Counter FREE_FILE_OPS = MetricsSystem.counter("FreeFileOps");
    private static final Counter GET_FILE_BLOCK_INFO_OPS =
        MetricsSystem.counter("GetFileBlockInfoOps");
    private static final Counter GET_FILE_INFO_OPS = MetricsSystem.counter("GetFileInfoOps");
    private static final Counter GET_NEW_BLOCK_OPS = MetricsSystem.counter("GetNewBlockOps");
    private static final Counter MOUNT_OPS = MetricsSystem.counter("MountOps");
    private static final Counter RENAME_PATH_OPS = MetricsSystem.counter("RenamePathOps");
    private static final Counter SET_ATTRIBUTE_OPS = MetricsSystem.counter("SetAttributeOps");
    private static final Counter UNMOUNT_OPS = MetricsSystem.counter("UnmountOps");

    public static final String FILES_PINNED = "FilesPinned";
    public static final String PATHS_TOTAL = "PathsTotal";
    public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
    public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
    public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";

    /**
     * Register some file system master related gauges.
     *
     * @param master the file system master
     * @param ufsManager the under filesystem manager
     */
    @VisibleForTesting
    public static void registerGauges(
        final FileSystemMaster master, final UfsManager ufsManager) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(FILES_PINNED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPinnedFiles();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(PATHS_TOTAL),
          () -> master.getNumberOfPaths());

      final String ufsDataFolder = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(UFS_CAPACITY_TOTAL),
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

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(UFS_CAPACITY_USED),
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

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(UFS_CAPACITY_FREE),
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
   * @throws AccessControlException if {@link AuthenticatedClientUser#getClientUser} finds that
   *         the thread-local user is null. Normally this should not happen.
   */
  private FileSystemMasterAuditContext createAuditContext(String command, AlluxioURI srcPath,
      @Nullable AlluxioURI dstPath, @Nullable Inode srcInode) throws AccessControlException {
    FileSystemMasterAuditContext auditContext =
        new FileSystemMasterAuditContext(mAsyncAuditLogWriter);
    if (mAsyncAuditLogWriter != null) {
      String user = AuthenticatedClientUser.getClientUser();
      String ugi;
      try {
        String primaryGroup = CommonUtils.getPrimaryGroupName(user);
        ugi = user + "," + primaryGroup;
      } catch (IOException e) {
        LOG.warn("Failed to get primary group for user {}.", user);
        ugi = user;
      }
      AuthType authType =
          Configuration.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      auditContext.setUgi(ugi)
          .setAuthType(authType)
          .setIp(FileSystemMasterClientServiceProcessor.getClientIp())
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

  private LockingScheme createLockingScheme(AlluxioURI path, CommonOptions options,
      InodeTree.LockMode desiredLockMode) {
    boolean shouldSync =
        mUfsSyncPathCache.shouldSyncPath(path.getPath(), options.getSyncIntervalMs());
    return new LockingScheme(path, desiredLockMode, shouldSync);
  }
}
