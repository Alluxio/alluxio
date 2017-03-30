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
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterRegistry;
import alluxio.master.ProtobufUtils;
import alluxio.master.block.BlockId;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.async.AsyncPersistHandler;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.InodeDirectoryIdGenerator;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeLockList;
import alluxio.master.file.meta.InodePathPair;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TempInodePathForChild;
import alluxio.master.file.meta.TempInodePathForDescendant;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketList;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.master.journal.JournalOutputStream;
import alluxio.metrics.MetricsSystem;
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
import alluxio.security.authorization.Mode;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemCommandOptions;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.PersistCommandOptions;
import alluxio.thrift.PersistFile;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerInfo;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The master that handles all file system metadata management.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMaster extends AbstractMaster {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMaster.class);
  private static final Set<Class<?>> DEPS = ImmutableSet.<Class<?>>of(BlockMaster.class);

  /**
   * Locking in the FileSystemMaster
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
   * Journaling in the FileSystemMaster
   *
   * Any changes to file system metadata that need to survive system restarts and / or failures
   * should be journaled. The intent to journal and the actual writing of the journal is decoupled
   * so that operations are not holding metadata locks waiting on the journal IO. In particular,
   * file system operations are expected to create a {@link JournalContext} resource, use it
   * throughout the lifetime of an operation to collect journal events through
   * {@link #appendJournalEntry(JournalEntry, JournalContext)}, and then close the resource, which
   * will persist the journal events. In order to ensure the journal events are always persisted,
   * the following paradigm is recommended:
   *
   * <p><blockquote><pre>
   *    try (JournalContext journalContext = createJournalContext()) {
   *      ...
   *    }
   * </pre></blockquote>
   *
   * NOTE: Because resources are released in the opposite order they are acquired, the
   * {@link JournalContext} resources should be always created before any {@link LockedInodePath}
   * resources to avoid holding an inode path lock while waiting for journal IO.
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
   * These methods perform the work from the public apis, and also asynchronously write to the
   * journal (for write operations). The names of these methods are suffixed with "AndJournal".
   * (B) cannot call (A)
   * (B) can call (B)
   * (B) can call (C)
   * (B) cannot call (D)
   *
   * (C) private (or package private) internal methods:
   * These methods perform the rest of the work, and do not do any journaling. The names of these
   * methods are suffixed by "Internal".
   * (C) cannot call (A)
   * (C) cannot call (B)
   * (C) can call (C)
   * (C) cannot call (D)
   *
   * (D) private FromEntry methods used to replay entries from the journal:
   * These methods are used to replay entries from reading the journal. This is done on start, as
   * well as for standby masters.
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

  private Future<List<AlluxioURI>> mStartupConsistencyCheck;

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory) {
    this(registry, journalFactory, ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.FILE_SYSTEM_MASTER_NAME, 3));
  }

  /**
   * Creates a new instance of {@link FileSystemMaster}.
   *
   * @param registry the master registry
   * @param journalFactory the factory for the journal to use for tracking master operations
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  public FileSystemMaster(MasterRegistry registry, JournalFactory journalFactory,
      ExecutorServiceFactory executorServiceFactory) {
    super(journalFactory.create(Constants.FILE_SYSTEM_MASTER_NAME), new SystemClock(),
        executorServiceFactory);

    mBlockMaster = registry.get(BlockMaster.class);
    mDirectoryIdGenerator = new InodeDirectoryIdGenerator(mBlockMaster);
    mMountTable = new MountTable();
    mInodeTree = new InodeTree(mBlockMaster, mDirectoryIdGenerator, mMountTable);

    // TODO(gene): Handle default config value for whitelist.
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.MASTER_WHITELIST, ","));

    mAsyncPersistHandler = AsyncPersistHandler.Factory.create(new FileSystemMasterView(this));
    mPermissionChecker = new PermissionChecker(mInodeTree);

    registry.add(FileSystemMaster.class, this);
    Metrics.registerGauges(this);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.FILE_SYSTEM_MASTER_CLIENT_SERVICE_NAME,
        new FileSystemMasterClientService.Processor<>(
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
  public Set<Class<?>> getDependencies() {
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
      try {
        unmountFromEntry(entry.getDeleteMountPoint());
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } else if (entry.hasAsyncPersistRequest()) {
      try {
        long fileId = (entry.getAsyncPersistRequest()).getFileId();
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          scheduleAsyncPersistenceInternal(inodePath);
        }
        // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
        mAsyncPersistHandler.scheduleAsyncPersistence(getPath(fileId));
      } catch (AlluxioException e) {
        // It's possible that rescheduling the async persist calls fails, because the blocks may no
        // longer be in the memory
        LOG.error(e.getMessage());
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    mInodeTree.streamToJournalCheckpoint(outputStream);
    outputStream.write(mDirectoryIdGenerator.toJournalEntry());
    // The mount table should be written to the checkpoint after the inodes are written, so that
    // when replaying the checkpoint, the inodes exist before mount entries. Replaying a mount
    // entry traverses the inode tree.
    mMountTable.streamToJournalCheckpoint(outputStream);
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    if (isLeader) {
      // Only initialize root when isLeader because when initializing root, BlockMaster needs to
      // write journal entry, if it is not leader, BlockMaster won't have a writable journal.
      // If it is standby, it should be able to load the inode tree from leader's checkpoint.
      mInodeTree.initializeRoot(SecurityUtils.getOwnerFromLoginModule(),
          SecurityUtils.getGroupFromLoginModule(), Mode.createFullAccess().applyDirectoryUMask());
      String defaultUFS = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      try {
        mMountTable.add(new AlluxioURI(MountTable.ROOT), new AlluxioURI(defaultUFS),
            MountOptions.defaults().setShared(
                UnderFileSystemUtils.isObjectStorage(defaultUFS) && Configuration
                    .getBoolean(PropertyKey.UNDERFS_OBJECT_STORE_MOUNT_SHARED_PUBLICLY)));
      } catch (FileAlreadyExistsException | InvalidPathException e) {
        throw new IOException("Failed to mount the default UFS " + defaultUFS);
      }
    }
    // Call super.start after mInodeTree is initialized because mInodeTree is needed to write
    // a journal entry during super.start. Call super.start before calling
    // getExecutorService() because the super.start initializes the executor service.
    super.start(isLeader);
    if (isLeader) {
      mTtlCheckerService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_TTL_CHECK, new MasterInodeTtlCheckExecutor(),
              Configuration.getInt(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS)));
      mLostFilesDetectionService = getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
              new LostFilesDetectionHeartbeatExecutor(),
              Configuration.getInt(PropertyKey.MASTER_HEARTBEAT_INTERVAL_MS)));
      if (Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
        mStartupConsistencyCheck = getExecutorService().submit(new Callable<List<AlluxioURI>>() {
          @Override
          public List<AlluxioURI> call() throws Exception {
            return startupCheckConsistency(ExecutorServiceFactories
                .fixedThreadPoolExecutorServiceFactory("startup-consistency-check", 32).create());
          }
        });
      }
    }
  }

  /**
   * Checks the consistency of the root in a multi-threaded and incremental fashion. This method
   * will only READ lock the directories and files actively being checked and release them after the
   * check on the file / directory is complete.
   *
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws InterruptedException if the thread is interrupted during execution
   * @throws IOException if an error occurs interacting with the under storage
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
       * @throws IOException if an error occurs interacting with the under storage
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
        Throwables.propagate(e);
      }
    }
    service.shutdown();
    return inconsistentUris;
  }

  /**
   * Class to represent the status and result of the startup consistency check.
   */
  public static final class StartupConsistencyCheck {
    /**
     * Status of the check.
     */
    public enum Status {
      COMPLETE,
      DISABLED,
      FAILED,
      RUNNING
    }

    /**
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     * @return a result set to the complete status
     */
    public static StartupConsistencyCheck complete(List<AlluxioURI> inconsistentUris) {
      return new StartupConsistencyCheck(Status.COMPLETE, inconsistentUris);
    }

    /**
     * @return a result set to the disabled status
     */
    public static StartupConsistencyCheck disabled() {
      return new StartupConsistencyCheck(Status.DISABLED, null);
    }

    /**
     * @return a result set to the failed status
     */
    public static StartupConsistencyCheck failed() {
      return new StartupConsistencyCheck(Status.FAILED, null);
    }

    /**
     * @return a result set to the running status
     */
    public static StartupConsistencyCheck running() {
      return new StartupConsistencyCheck(Status.RUNNING, null);
    }

    private Status mStatus;
    private List<AlluxioURI> mInconsistentUris;

    /**
     * Create a new startup consistency check result.
     *
     * @param status the state of the check
     * @param inconsistentUris the uris which are inconsistent with the underlying storage
     */
    private StartupConsistencyCheck(Status status, List<AlluxioURI> inconsistentUris) {
      mStatus = status;
      mInconsistentUris = inconsistentUris;
    }

    /**
     * @return the status of the check
     */
    public Status getStatus() {
      return mStatus;
    }

    /**
     * @return the uris which are inconsistent with the underlying storage
     */
    public List<AlluxioURI> getInconsistentUris() {
      return mInconsistentUris;
    }
  }

  /**
   * @return the status of the startup consistency check and inconsistent paths if it is complete
   */
  public StartupConsistencyCheck getStartupConsistencyCheck() {
    if (!Configuration.getBoolean(PropertyKey.MASTER_STARTUP_CONSISTENCY_CHECK_ENABLED)) {
      return StartupConsistencyCheck.disabled();
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

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   * <p>
   * This operation requires users to have {@link Mode.Bits#READ} permission of the path.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   */
  public long getFileId(AlluxioURI path) throws AccessControlException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExistAndJournal(inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(true), journalContext);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return inodePath.getInode().getId();
    } catch (InvalidPathException | FileDoesNotExistException e) {
      return IdUtils.INVALID_FILE_ID;
    }
  }

  /**
   * Returns the {@link FileInfo} for a given file id. This method is not user-facing but supposed
   * to be called by other internal servers (e.g., block workers, lineage master, web UI).
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  // TODO(binfan): Add permission checking for internal APIs
  public FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return getFileInfoInternal(inodePath);
    }
  }

  /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have {@link Mode.Bits#READ} permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  // TODO(peis): Add an option not to load metadata.
  public FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_FILE_INFO_OPS.inc();

    // Get a READ lock first to see if we need to load metadata, note that this assumes load
    // metadata for direct children is disabled by default.
    try (LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      if (inodePath.fullPathExists()) {
        // The file already exists, so metadata does not need to be loaded.
        return getFileInfoInternal(inodePath);
      }
    }

    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      loadMetadataIfNotExistAndJournal(inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(true), journalContext);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      return getFileInfoInternal(inodePath);
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given inode
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  private FileInfo getFileInfoInternal(LockedInodePath inodePath)
      throws FileDoesNotExistException, AccessControlException {
    Inode<?> inode = inodePath.getInode();
    AlluxioURI uri = inodePath.getUri();
    FileInfo fileInfo = inode.generateClientFileInfo(uri.toString());
    fileInfo.setInMemoryPercentage(getInMemoryPercentage(inode));
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
    // Only set the UFS path if the path is nested under a mount point.
    if (!uri.equals(resolvedUri)) {
      fileInfo.setUfsPath(resolvedUri.toString());
    }
    Metrics.FILE_INFOS_GOT.inc();
    return fileInfo;
  }

  /**
   * Returns the persistence state for a file id. This method is used by the lineage master.
   *
   * @param fileId the file id
   * @return the {@link PersistenceState} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   */
  // TODO(binfan): Add permission checking for internal APIs
  public PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException {
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      return inodePath.getInode().getPersistenceState();
    }
  }

  /**
   * Returns a list of {@link FileInfo} for a given path. If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * <p>
   * This operation requires users to have
   * {@link Mode.Bits#READ} permission on the path, and also
   * {@link Mode.Bits#EXECUTE} permission on the path if it is a directory.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @param listStatusOptions the {@link alluxio.master.file.options.ListStatusOptions}
   * @return the list of {@link FileInfo}s
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  public List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException {
    Metrics.GET_FILE_INFO_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      // This is WRITE locked, since loading metadata is possible.
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);

      LoadMetadataOptions loadMetadataOptions =
          LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(
              listStatusOptions.getLoadMetadataType() != LoadMetadataType.Never);
      Inode<?> inode;
      if (inodePath.fullPathExists()) {
        inode = inodePath.getInode();
        if (inode.isDirectory()
            && listStatusOptions.getLoadMetadataType() != LoadMetadataType.Always
            && ((InodeDirectory) inode).isDirectChildrenLoaded()) {
          loadMetadataOptions.setLoadDirectChildren(false);
        }
      }

      loadMetadataIfNotExistAndJournal(inodePath, loadMetadataOptions, journalContext);
      mInodeTree.ensureFullInodePath(inodePath, InodeTree.LockMode.READ);
      inode = inodePath.getInode();

      List<FileInfo> ret = new ArrayList<>();
      if (inode.isDirectory()) {
        TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
        mPermissionChecker.checkPermission(Mode.Bits.EXECUTE, inodePath);
        for (Inode<?> child : ((InodeDirectory) inode).getChildren()) {
          child.lockReadAndCheckParent(inode);
          try {
            // the path to child for getPath should already be locked.
            tempInodePath.setDescendant(child, mInodeTree.getPath(child));
            ret.add(getFileInfoInternal(tempInodePath));
          } finally {
            child.unlockRead();
          }
        }
      } else {
        ret.add(getFileInfoInternal(inodePath));
      }
      Metrics.FILE_INFOS_GOT.inc();
      return ret;
    }
  }

  /**
   * @return a read-only view of the file system master
   */
  public FileSystemMasterView getFileSystemMasterView() {
    return new FileSystemMasterView(this);
  }

  /**
   * Checks the consistency of the files and directories in the subtree under the path.
   *
   * @param path the root of the subtree to check
   * @param options the options to use for the checkConsistency method
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws AccessControlException if the permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws IOException if an error occurs interacting with the under storage
   */
  public List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException {
    List<AlluxioURI> inconsistentUris = new ArrayList<>();
    try (LockedInodePath parent = mInodeTree.lockInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, parent);
      try (InodeLockList children = mInodeTree.lockDescendants(parent, InodeTree.LockMode.READ)) {
        if (!checkConsistencyInternal(parent.getInode(), parent.getUri())) {
          inconsistentUris.add(parent.getUri());
        }
        for (Inode child : children.getInodes()) {
          AlluxioURI currentPath = mInodeTree.getPath(child);
          if (!checkConsistencyInternal(child, currentPath)) {
            inconsistentUris.add(currentPath);
          }
        }
      }
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
    UnderFileSystem ufs = resolution.getUfs();
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
      return ufs.isFile(ufsPath) && ufs.getFileSize(ufsPath) == file.getLength();
    }
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the path.
   *
   * @param path the file path to complete
   * @param options the method options
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws AccessControlException if permission checking fails
   */
  public void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException {
    Metrics.COMPLETE_FILE_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      // Even readonly mount points should be able to complete a file, for UFS reads in CACHE mode.
      completeFileAndJournal(inodePath, options, journalContext);
    }
  }

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} to complete
   * @param options the method options
   * @param journalContext the journal context
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if an invalid file size is encountered
   */
  private void completeFileAndJournal(LockedInodePath inodePath, CompleteFileOptions options,
      JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException {
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
    long inMemoryLength = 0;
    long fileBlockSize = fileInode.getBlockSizeBytes();
    for (int i = 0; i < blockInfoList.size(); i++) {
      BlockInfo blockInfo = blockInfoList.get(i);
      inMemoryLength += blockInfo.getLength();
      if (i < blockInfoList.size() - 1 && blockInfo.getLength() != fileBlockSize) {
        throw new BlockInfoException(
            "Block index " + i + " has a block size smaller than the file block size (" + fileInode
                .getBlockSizeBytes() + ")");
      }
    }

    // If the file is persisted, its length is determined by UFS. Otherwise, its length is
    // determined by its memory footprint.
    long length = fileInode.isPersisted() ? options.getUfsLength() : inMemoryLength;

    completeFileInternal(fileInode.getBlockIds(), inodePath, length, options.getOperationTimeMs());
    CompleteFileEntry completeFileEntry =
        CompleteFileEntry.newBuilder().addAllBlockIds(fileInode.getBlockIds()).setId(inode.getId())
            .setLength(length).setOpTimeMs(options.getOperationTimeMs()).build();
    appendJournalEntry(JournalEntry.newBuilder().setCompleteFile(completeFileEntry).build(),
        journalContext);
  }

  /**
   * @param blockIds the block ids to use
   * @param inodePath the {@link LockedInodePath} to complete
   * @param length the length to use
   * @param opTimeMs the operation time (in milliseconds)
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  private void completeFileInternal(List<Long> blockIds, LockedInodePath inodePath, long length,
      long opTimeMs)
      throws FileDoesNotExistException, InvalidPathException, InvalidFileSizeException,
      FileAlreadyCompletedException {
    InodeFile inode = inodePath.getInodeFile();
    inode.setBlockIds(blockIds);
    inode.setLastModificationTimeMs(opTimeMs);
    inode.complete(length);

    if (inode.isPersisted()) {
      // Commit all the file blocks (without locations) so the metadata for the block exists.
      long currLength = length;
      for (long blockId : inode.getBlockIds()) {
        long blockSize = Math.min(currLength, inode.getBlockSizeBytes());
        mBlockMaster.commitBlockInUFS(blockId, blockSize);
        currLength -= blockSize;
      }
    }
    Metrics.FILES_COMPLETED.inc();
  }

  /**
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file has already been completed
   */
  private void completeFileFromEntry(CompleteFileEntry entry)
      throws InvalidPathException, InvalidFileSizeException, FileAlreadyCompletedException {
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      completeFileInternal(entry.getBlockIdsList(), inodePath, entry.getLength(),
          entry.getOpTimeMs());
    } catch (FileDoesNotExistException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * This operation requires {@link Mode.Bits#WRITE} permission on the parent of this path.
   *
   * @param path the file to create
   * @param options method options
   * @return the id of the created file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information is encountered
   * @throws IOException if the creation fails
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   * option is false
   */
  public long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException {
    Metrics.CREATE_FILES_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      createFileAndJournal(inodePath, options, journalContext);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the file to create
   * @param options method options
   * @param journalContext the journal context
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information in encountered
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if the creation fails
   */
  private void createFileAndJournal(LockedInodePath inodePath, CreateFileOptions options,
      JournalContext journalContext)
      throws FileAlreadyExistsException, BlockInfoException, FileDoesNotExistException,
      InvalidPathException, IOException {
    InodeTree.CreatePathResult createResult = createFileInternal(inodePath, options);
    journalCreatePathResult(createResult, journalContext);
  }

  /**
   * @param inodePath the path to be created
   * @param options method options
   * @return {@link InodeTree.CreatePathResult} with the path creation result
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if invalid block information is encountered
   * @throws IOException if an I/O error occurs
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  InodeTree.CreatePathResult createFileInternal(LockedInodePath inodePath,
      CreateFileOptions options)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      FileDoesNotExistException {
    InodeTree.CreatePathResult createResult = mInodeTree.createPath(inodePath, options);
    // If the create succeeded, the list of created inodes will not be empty.
    List<Inode<?>> created = createResult.getCreated();
    InodeFile inode = (InodeFile) created.get(created.size() - 1);
    if (mWhitelist.inList(inodePath.getUri().toString())) {
      inode.setCacheable(true);
    }

    mTtlBuckets.insert(inode);

    Metrics.FILES_CREATED.inc();
    Metrics.DIRECTORIES_CREATED.inc();
    return createResult;
  }

  /**
   * Reinitializes the blocks of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @param ttlAction action to take after Ttl expiry
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  // Used by lineage master
  public long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      long id = mInodeTree.reinitializeFile(inodePath, blockSizeBytes, ttl, ttlAction);
      ReinitializeFileEntry reinitializeFile =
          ReinitializeFileEntry.newBuilder().setPath(path.getPath())
              .setBlockSizeBytes(blockSizeBytes).setTtl(ttl)
              .setTtlAction(ProtobufUtils.toProtobuf(ttlAction)).build();
      appendJournalEntry(
          JournalEntry.newBuilder().setReinitializeFile(reinitializeFile).build(), journalContext);
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

  /**
   * Gets a new block id for the next block of a given file to write to.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the path as
   * this API is called when creating a new block for a file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   * @throws AccessControlException if permission checking fails
   */
  public long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_NEW_BLOCK_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkPermission(Mode.Bits.WRITE, inodePath);
      Metrics.NEW_BLOCKS_GOT.inc();
      return inodePath.getInodeFile().getNewBlockId();
    }
  }

  /**
   * @return a copy of the current mount table
   */
  public Map<String, MountInfo> getMountTable() {
    return mMountTable.getMountTable();
  }

  /**
   * @return the number of files and directories
   */
  public int getNumberOfPaths() {
    return mInodeTree.getSize();
  }

  /**
   * @return the number of pinned files and directories
   */
  public int getNumberOfPinnedFiles() {
    return mInodeTree.getPinnedSize();
  }

  /**
   * Deletes a given path.
   * <p>
   * This operation requires user to have {@link Mode.Bits#WRITE}
   * permission on the parent of the path.
   *
   * @param path the path to delete
   * @param options method options
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  public void delete(AlluxioURI path, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
      AccessControlException {
    Metrics.DELETE_PATHS_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
         LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      deleteAndJournal(inodePath, options, journalContext);
    }
  }

  /**
   * Deletes a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the path to delete
   * @param deleteOptions the method options
   * @param journalContext the journal context
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  private void deleteAndJournal(LockedInodePath inodePath, DeleteOptions deleteOptions,
      JournalContext journalContext) throws InvalidPathException, FileDoesNotExistException,
      IOException, DirectoryNotEmptyException {
    Inode<?> inode = inodePath.getInode();
    long fileId = inode.getId();
    long opTimeMs = System.currentTimeMillis();
    deleteInternal(inodePath, false, opTimeMs, deleteOptions);
    DeleteFileEntry deleteFile = DeleteFileEntry.newBuilder().setId(fileId)
        .setRecursive(deleteOptions.isRecursive()).setOpTimeMs(opTimeMs).build();
    appendJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build(), journalContext);
  }

  /**
   * @param entry the entry to use
   */
  private void deleteFromEntry(DeleteFileEntry entry) {
    Metrics.DELETE_PATHS_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      deleteInternal(inodePath, true, entry.getOpTimeMs(), DeleteOptions.defaults()
          .setRecursive(entry.getRecursive()).setAlluxioOnly(entry.getAlluxioOnly()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for avoiding {@link DirectoryNotEmptyException} when calling
   * {@link #deleteInternal(LockedInodePath, boolean, long, DeleteOptions)}.
   *
   * @param inodePath the {@link LockedInodePath} to delete
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if the fileId is for the root directory
   * @throws IOException if an I/O error is encountered
   */
  private void deleteRecursiveInternal(LockedInodePath inodePath, boolean replayed, long opTimeMs,
      DeleteOptions deleteOptions) throws FileDoesNotExistException, IOException,
      InvalidPathException {
    try {
      deleteInternal(inodePath, replayed, opTimeMs, deleteOptions);
    } catch (DirectoryNotEmptyException e) {
      throw new IllegalStateException(
          "deleteInternal should never throw DirectoryNotEmptyException when recursive is true", e);
    }
  }

  /**
   * Implements file deletion.
   *
   * @param inodePath the file {@link LockedInodePath}
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @param deleteOptions the method optitions
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   * @throws InvalidPathException if the specified path is the root
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  private void deleteInternal(LockedInodePath inodePath, boolean replayed, long opTimeMs,
      DeleteOptions deleteOptions) throws FileDoesNotExistException, IOException,
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
    boolean alluxioOnly = deleteOptions.isAlluxioOnly();
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

    List<Inode<?>> delInodes = new ArrayList<>();
    delInodes.add(inode);

    try (InodeLockList lockList = mInodeTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
      delInodes.addAll(lockList.getInodes());

      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
      // We go through each inode, removing it from its parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (int i = delInodes.size() - 1; i >= 0; i--) {
        Inode<?> delInode = delInodes.get(i);
        // the path to delInode for getPath should already be locked.
        AlluxioURI alluxioUriToDel = mInodeTree.getPath(delInode);
        tempInodePath.setDescendant(delInode, alluxioUriToDel);

        // TODO(jiri): What should the Alluxio behavior be when a UFS delete operation fails?
        // Currently, it will result in an inconsistency between Alluxio and UFS.
        if (!replayed && delInode.isPersisted()) {
          try {
            // If this is a mount point, we have deleted all the children and can unmount it
            // TODO(calvin): Add tests (ALLUXIO-1831)
            if (mMountTable.isMountPoint(alluxioUriToDel)) {
              unmountInternal(alluxioUriToDel);
            } else {
              // Delete the file in the under file system.
              MountTable.Resolution resolution = mMountTable.resolve(alluxioUriToDel);
              String ufsUri = resolution.getUri().toString();
              UnderFileSystem ufs = resolution.getUfs();
              boolean failedToDelete = false;
              if (!alluxioOnly) {
                if (delInode.isFile()) {
                  if (!ufs.deleteFile(ufsUri)) {
                    failedToDelete = ufs.isFile(ufsUri);
                    if (!failedToDelete) {
                      LOG.warn("The file to delete does not exist in ufs: {}", ufsUri);
                    }
                  }
                } else {
                  if (!ufs.deleteDirectory(ufsUri, alluxio.underfs.options.DeleteOptions
                      .defaults().setRecursive(true))) {
                    failedToDelete = ufs.isDirectory(ufsUri);
                    if (!failedToDelete) {
                      LOG.warn("The directory to delete does not exist in ufs: {}", ufsUri);
                    }
                  }
                }
                if (failedToDelete) {
                  LOG.error("Failed to delete {} from the under filesystem", ufsUri);
                  throw new IOException(ExceptionMessage.DELETE_FAILED_UFS.getMessage(ufsUri));
                }
              }
            }
          } catch (InvalidPathException e) {
            LOG.warn(e.getMessage());
          }
        }

        if (delInode.isFile()) {
          // Remove corresponding blocks from workers and delete metadata in master.
          mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds(), true /* delete */);
        }

        mInodeTree.deleteInode(tempInodePath, opTimeMs);
      }
    }

    Metrics.PATHS_DELETED.inc(delInodes.size());
  }

  /**
   * Gets the {@link FileBlockInfo} for all blocks of a file. If path is a directory, an exception
   * is thrown.
   * <p>
   * This operation requires the client user to have {@link Mode.Bits#READ} permission on the
   * the path.
   *
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given path
   * @throws FileDoesNotExistException if the file does not exist or path is a directory
   * @throws InvalidPathException if the path of the given file is invalid
   * @throws AccessControlException if permission checking fails
   */
  public List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    Metrics.GET_FILE_BLOCK_INFO_OPS.inc();
    try (LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.READ)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      List<FileBlockInfo> ret = getFileBlockInfoListInternal(inodePath);
      Metrics.FILE_BLOCK_INFOS_GOT.inc();
      return ret;
    }
  }

  /**
   * @param inodePath the {@link LockedInodePath} to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given inode
   * @throws InvalidPathException if the path of the given file is invalid
   */
  private List<FileBlockInfo> getFileBlockInfoListInternal(LockedInodePath inodePath)
      throws InvalidPathException, FileDoesNotExistException {
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
    fileBlockInfo.setUfsLocations(new ArrayList<String>());

    // The sequence number part of the block id is the block index.
    long offset = file.getBlockSizeBytes() * BlockId.getSequenceNumber(blockInfo.getBlockId());
    fileBlockInfo.setOffset(offset);

    if (fileBlockInfo.getBlockInfo().getLocations().isEmpty() && file.isPersisted()) {
      // No alluxio locations, but there is a checkpoint in the under storage system. Add the
      // locations from the under storage system.
      MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
      String ufsUri = resolution.getUri().toString();
      UnderFileSystem ufs = resolution.getUfs();
      List<String> locs;
      try {
        locs = ufs.getFileLocations(ufsUri,
            FileLocationOptions.defaults().setOffset(fileBlockInfo.getOffset()));
      } catch (IOException e) {
        return fileBlockInfo;
      }
      if (locs != null) {
        for (String loc : locs) {
          fileBlockInfo.getUfsLocations().add(loc);
        }
      }
    }
    return fileBlockInfo;
  }

  /**
   * Returns whether the inodeFile is fully in memory or not. The file is fully in memory only if
   * all the blocks of the file are in memory, in other words, the in memory percentage is 100.
   *
   * @return true if the file is fully in memory, false otherwise
   */
  private boolean isFullyInMemory(InodeFile inode) {
    return getInMemoryPercentage(inode) == 100;
  }

  /**
   * @return absolute paths of all in memory files
   */
  public List<AlluxioURI> getInMemoryFiles() {
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
   * Adds in memory files to the array list passed in. This method assumes the inode passed in is
   * already read locked.
   *
   * @param inode the root of the subtree to search
   * @param uri the uri of the parent of the inode
   * @param files the list to accumulate the results in
   */
  private void getInMemoryFilesInternal(Inode<?> inode, AlluxioURI uri, List<AlluxioURI> files) {
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
   * Gets the in-memory percentage of an Inode. For a file that has all blocks in memory, it returns
   * 100; for a file that has no block in memory, it returns 0. Returns 0 for a directory.
   *
   * @param inode the inode
   * @return the in memory percentage
   */
  private int getInMemoryPercentage(Inode<?> inode) {
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

  /**
   * Creates a directory for a given path.
   * <p>
   * This operation requires the client user to have
   * {@link Mode.Bits#WRITE} permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param options method options
   * @return the id of the created directory
   * @throws InvalidPathException when the path is invalid, please see documentation on {@link
   *         InodeTree#createPath(LockedInodePath, alluxio.master.file.options.CreatePathOptions)}
   *         for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  public long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    LOG.debug("createDirectory {} ", path);
    Metrics.CREATE_DIRECTORIES_OPS.inc();

    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      createDirectoryAndJournal(inodePath, options, journalContext);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Creates a directory for a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} of the directory
   * @param options method options
   * @param journalContext the journal context
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   * @throws InvalidPathException when the path is invalid, please see documentation on {@link
   *         InodeTree#createPath(LockedInodePath, alluxio.master.file.options.CreatePathOptions)}
   *         for more details
   * @throws AccessControlException if permission checking fails
   * @throws IOException if a non-Alluxio related exception occurs
   */
  private void createDirectoryAndJournal(LockedInodePath inodePath, CreateDirectoryOptions options,
      JournalContext journalContext)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException {
    InodeTree.CreatePathResult createResult = createDirectoryInternal(inodePath, options);
    journalCreatePathResult(createResult, journalContext);
    Metrics.DIRECTORIES_CREATED.inc();
  }

  /**
   * Implementation of directory creation for a given path.
   *
   * @param inodePath the path of the directory
   * @param options method options
   * @return an {@link alluxio.master.file.meta.InodeTree.CreatePathResult} representing the
   *         modified inodes and created inodes during path creation
   * @throws InvalidPathException when the path is invalid, please see documentation on {@link
   *         InodeTree#createPath(LockedInodePath, alluxio.master.file.options.CreatePathOptions)}
   *         for more details
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   */
  private InodeTree.CreatePathResult createDirectoryInternal(LockedInodePath inodePath,
      CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException {
    try {
      InodeTree.CreatePathResult createResult = mInodeTree.createPath(inodePath, options);
      InodeDirectory inodeDirectory = (InodeDirectory) inodePath.getInode();
      // If inodeDirectory's ttl not equals Constants.NO_TTL, it should insert into mTtlBuckets
      if (createResult.getCreated().size() > 0) {
        mTtlBuckets.insert(inodeDirectory);
      }

      return createResult;
    } catch (BlockInfoException e) {
      // Since we are creating a directory, the block size is ignored, no such exception should
      // happen.
      Throwables.propagate(e);
    }
    return null;
  }

  /**
   * Journals the {@link InodeTree.CreatePathResult}. This does not flush the journal.
   * Synchronization is required outside of this method.
   *
   * @param createResult the {@link InodeTree.CreatePathResult} to journal
   * @param journalContext the journalContext
   */
  private void journalCreatePathResult(InodeTree.CreatePathResult createResult,
      JournalContext journalContext) {
    for (Inode<?> inode : createResult.getModified()) {
      InodeLastModificationTimeEntry inodeLastModificationTime =
          InodeLastModificationTimeEntry.newBuilder().setId(inode.getId())
              .setLastModificationTimeMs(inode.getLastModificationTimeMs()).build();
      appendJournalEntry(
          JournalEntry.newBuilder().setInodeLastModificationTime(inodeLastModificationTime).build(),
          journalContext);
    }
    boolean createdDir = false;
    for (Inode<?> inode : createResult.getCreated()) {
      appendJournalEntry(inode.toJournalEntry(), journalContext);
      if (inode.isDirectory()) {
        createdDir = true;
      }
    }
    if (createdDir) {
      // At least one directory was created, so journal the state of the directory id generator.
      appendJournalEntry(mDirectoryIdGenerator.toJournalEntry(), journalContext);
    }
    for (Inode<?> inode : createResult.getPersisted()) {
      PersistDirectoryEntry persistDirectory =
          PersistDirectoryEntry.newBuilder().setId(inode.getId()).build();
      appendJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build(),
          journalContext);
    }
  }

  /**
   * Renames a file to a destination.
   * <p>
   * This operation requires users to have
   * {@link Mode.Bits#WRITE} permission on the parent of the src path, and
   * {@link Mode.Bits#WRITE} permission on the parent of the dst path.
   *
   * @param srcPath the source path to rename
   * @param dstPath the destination path to rename the file to
   * @param options method options
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileAlreadyExistsException if the file already exists
   */
  public void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.RENAME_PATH_OPS.inc();
    // Require a WRITE lock on the source but only a READ lock on the destination. Since the
    // destination should not exist, we will only obtain a READ lock on the destination parent. The
    // modify operations on the parent inodes are thread safe so WRITE locks are not required.
    try (JournalContext journalContext = createJournalContext();
        InodePathPair inodePathPair = mInodeTree
            .lockInodePathPair(srcPath, InodeTree.LockMode.WRITE, dstPath,
                InodeTree.LockMode.READ)) {
      LockedInodePath srcInodePath = inodePathPair.getFirst();
      LockedInodePath dstInodePath = inodePathPair.getSecond();
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, srcInodePath);
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, dstInodePath);
      mMountTable.checkUnderWritableMountPoint(srcPath);
      mMountTable.checkUnderWritableMountPoint(dstPath);
      renameAndJournal(srcInodePath, dstInodePath, options, journalContext);
      LOG.debug("Renamed {} to {}", srcPath, dstPath);
    }
  }

  /**
   * Renames a file to a destination.
   * <p>
   * Writes to the journal.
   *
   * @param srcInodePath the source path to rename
   * @param dstInodePath the destination path to rename the file to
   * @param options method options
   * @param journalContext the journalContext
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws IOException if an I/O error occurs
   */
  private void renameAndJournal(LockedInodePath srcInodePath, LockedInodePath dstInodePath,
      RenameOptions options, JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException {
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
    renameInternal(srcInodePath, dstInodePath, false, options);
    List<Inode<?>> persistedInodes = propagatePersistedInternal(srcInodePath, false);
    journalPersistedInodes(persistedInodes, journalContext);

    RenameEntry rename =
        RenameEntry.newBuilder().setId(srcInode.getId()).setDstPath(dstInodePath.getUri().getPath())
            .setOpTimeMs(options.getOperationTimeMs()).build();
    appendJournalEntry(JournalEntry.newBuilder().setRename(rename).build(), journalContext);
  }

  /**
   * Implements renaming.
   *
   * @param srcInodePath the path of the rename source
   * @param dstInodePath the path to the rename destination
   * @param replayed whether the operation is a result of replaying the journal
   * @param options method options
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error is encountered
   */
  private void renameInternal(LockedInodePath srcInodePath, LockedInodePath dstInodePath,
      boolean replayed, RenameOptions options)
      throws FileDoesNotExistException, InvalidPathException, IOException {

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
        MountTable.Resolution resolution = mMountTable.resolve(srcPath);

        String ufsSrcPath = resolution.getUri().toString();
        UnderFileSystem ufs = resolution.getUfs();
        String ufsDstUri = mMountTable.resolve(dstPath).getUri().toString();
        // Create ancestor directories from top to the bottom. We cannot use recursive create
        // parents here because the permission for the ancestors can be different.
        List<Inode<?>> dstInodeList = dstInodePath.getInodeList();
        Stack<Pair<String, MkdirsOptions>> ufsDirsToMakeWithOptions = new Stack<>();
        AlluxioURI curUfsDirPath = new AlluxioURI(ufsDstUri).getParent();
        // The dst inode does not exist yet, so the last inode in the list is the existing parent.
        for (int i = dstInodeList.size() - 1; i >= 0; i--) {
          if (ufs.isDirectory(curUfsDirPath.toString())) {
            break;
          }
          Inode<?> curInode = dstInodeList.get(i);
          MkdirsOptions mkdirsOptions =
              MkdirsOptions.defaults().setCreateParent(false).setOwner(curInode.getOwner())
                  .setGroup(curInode.getGroup()).setMode(new Mode(curInode.getMode()));
          ufsDirsToMakeWithOptions.push(new Pair<>(curUfsDirPath.toString(), mkdirsOptions));
          curUfsDirPath = curUfsDirPath.getParent();
        }
        while (!ufsDirsToMakeWithOptions.empty()) {
          Pair<String, MkdirsOptions> ufsDirAndPerm = ufsDirsToMakeWithOptions.pop();
          if (!ufs.mkdirs(ufsDirAndPerm.getFirst(), ufsDirAndPerm.getSecond())) {
            throw new IOException(
                ExceptionMessage.FAILED_UFS_CREATE.getMessage(ufsDirAndPerm.getFirst()));
          }
        }
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
      renameInternal(srcInodePath, dstInodePath, true, options);
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
        persistedInodes.add(inode);
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
      appendJournalEntry(JournalEntry.newBuilder().setPersistDirectory(persistDirectory).build(),
          journalContext);
    }
  }

  // TODO(binfan): throw a better exception rather than UnexpectedAlluxioException. Currently
  // UnexpectedAlluxioException is thrown because we want to keep backwards compatibility with
  // clients of earlier versions prior to 1.5. If a new exception is added, it will be converted
  // into RuntimeException on the client.

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * <p>
   * This operation requires users to have {@link Mode.Bits#READ} permission on the path.
   *
   * @param path the path to free
   * @param options options to free
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  public void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException {
    Metrics.FREE_FILE_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
      freeAndJournal(inodePath, options, journalContext);
    }
  }

  /**
   * Implements free operation.
   * <p>
   * This may write to the journal as free operation may change the pinned state of inodes.
   *
   * @param inodePath inode of the path to free
   * @param options options to free
   * @param journalContext the journal context
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  private void freeAndJournal(LockedInodePath inodePath, FreeOptions options,
      JournalContext journalContext)
      throws FileDoesNotExistException, UnexpectedAlluxioException, AccessControlException,
      InvalidPathException {
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

    try (InodeLockList lockList = mInodeTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
      freeInodes.addAll(lockList.getInodes());
      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
      // We go through each inode.
      for (int i = freeInodes.size() - 1; i >= 0; i--) {
        Inode<?> freeInode = freeInodes.get(i);

        if (freeInode.isFile()) {
          if (freeInode.getPersistenceState() != PersistenceState.PERSISTED) {
            throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
                .getMessage(mInodeTree.getPath(freeInode)));
          }
          if (freeInode.isPinned()) {
            if (!options.isForced()) {
              throw new UnexpectedAlluxioException(ExceptionMessage.CANNOT_FREE_PINNED_FILE
                  .getMessage(mInodeTree.getPath(freeInode)));
            }
            // the path to inode for getPath should already be locked.
            tempInodePath.setDescendant(freeInode, mInodeTree.getPath(freeInode));
            SetAttributeOptions setAttributeOptions =
                SetAttributeOptions.defaults().setRecursive(false).setPinned(false);
            setAttributeInternal(tempInodePath, false, opTimeMs, setAttributeOptions);
            journalSetAttribute(tempInodePath, opTimeMs, setAttributeOptions, journalContext);
          }
          // Remove corresponding blocks from workers.
          mBlockMaster.removeBlocks(((InodeFile) freeInode).getBlockIds(), false /* delete */);
        }
      }
    }
    Metrics.FILES_FREED.inc(freeInodes.size());
  }

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public AlluxioURI getPath(long fileId) throws FileDoesNotExistException {
    try (
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(fileId, InodeTree.LockMode.READ)) {
      // the path is already locked.
      return mInodeTree.getPath(inodePath.getInode());
    }
  }

  /**
   * @return the set of inode ids which are pinned
   */
  public Set<Long> getPinIdList() {
    return mInodeTree.getPinIdSet();
  }

  /**
   * @return the ufs address for this master
   */
  public String getUfsAddress() {
    return Configuration.get(PropertyKey.UNDERFS_ADDRESS);
  }

  /**
   * @return the white list
   */
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  /**
   * @return all the files lost on the workers
   */
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
   * Reports a file as lost.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public void reportLostFile(long fileId) throws FileDoesNotExistException {
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

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the path
   * and its parent path if path is a file, or {@link Mode.Bits#WRITE} permission on the
   * parent path if path is a directory.
   *
   * @param path the path for which metadata should be loaded
   * @param options the load metadata options
   * @return the file id of the loaded path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   */
  public long loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, IOException, AccessControlException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      loadMetadataAndJournal(inodePath, options, journalContext);
      return inodePath.getInode().getId();
    }
  }

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the path for which metadata should be loaded
   * @param options the load metadata options
   * @param journalContext the journal context
   * @throws InvalidPathException if invalid path is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws AccessControlException if permission checking fails
   * @throws IOException if an I/O error occurs
   */
  private void loadMetadataAndJournal(LockedInodePath inodePath, LoadMetadataOptions options,
      JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, BlockInfoException,
      FileAlreadyCompletedException, InvalidFileSizeException, AccessControlException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();
    try {
      if (options.getUnderFileStatus() == null && !ufs.exists(ufsUri.toString())) {
        // uri does not exist in ufs
        InodeDirectory inode = (InodeDirectory) inodePath.getInode();
        inode.setDirectChildrenLoaded(true);
      }
      boolean isFile;
      if (options.getUnderFileStatus() != null) {
        isFile = options.getUnderFileStatus().isFile();
      } else {
        isFile = ufs.isFile(ufsUri.toString());
      }
      if (isFile) {
        loadFileMetadataAndJournal(inodePath, resolution, options, journalContext);
      } else {
        loadDirectoryMetadataAndJournal(inodePath, options, journalContext);
        InodeDirectory inode = (InodeDirectory) inodePath.getInode();

        if (options.isLoadDirectChildren()) {
          UnderFileStatus[] files = ufs.listStatus(ufsUri.toString());
          for (UnderFileStatus file : files) {
            if (PathUtils.isTemporaryFileName(file.getName())
                || inode.getChild(file.getName()) != null) {
              continue;
            }
            TempInodePathForChild tempInodePath =
                new TempInodePathForChild(inodePath, file.getName());
            LoadMetadataOptions loadMetadataOptions =
                LoadMetadataOptions.defaults().setLoadDirectChildren(false)
                    .setCreateAncestors(false).setUnderFileStatus(file);
            loadMetadataAndJournal(tempInodePath, loadMetadataOptions, journalContext);
          }
          inode.setDirectChildrenLoaded(true);
        }
      }
    } catch (IOException e) {
      LOG.error(ExceptionUtils.getStackTrace(e));
      throw e;
    }
  }

  /**
   * Loads metadata for the file identified by the given path from UFS into Alluxio.
   *
   * @param inodePath the path for which metadata should be loaded
   * @param resolution the UFS resolution of path
   * @param options the load metadata options
   * @param journalContext the journal context
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws AccessControlException if permission checking fails or permission setting fails
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws IOException if an I/O error occurs
   */
  private void loadFileMetadataAndJournal(LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataOptions options, JournalContext journalContext)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      AccessControlException, FileAlreadyCompletedException, InvalidFileSizeException, IOException {
    if (inodePath.fullPathExists()) {
      return;
    }
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();

    long ufsBlockSizeByte = ufs.getBlockSizeByte(ufsUri.toString());
    long ufsLength = ufs.getFileSize(ufsUri.toString());
    // Metadata loaded from UFS has no TTL set.
    CreateFileOptions createFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(ufsBlockSizeByte)
            .setRecursive(options.isCreateAncestors()).setMetadataLoad(true).setPersisted(true);
    String ufsOwner = ufs.getOwner(ufsUri.toString());
    String ufsGroup = ufs.getGroup(ufsUri.toString());
    short ufsMode = ufs.getMode(ufsUri.toString());
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileOptions = createFileOptions.setOwner(ufsOwner).setGroup(ufsGroup).setMode(mode);

    try {
      createFileAndJournal(inodePath, createFileOptions, journalContext);
      CompleteFileOptions completeOptions = CompleteFileOptions.defaults().setUfsLength(ufsLength);
      completeFileAndJournal(inodePath, completeOptions, journalContext);
    } catch (FileAlreadyExistsException e) {
      LOG.error("FileAlreadyExistsException seen unexpectedly.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads metadata for the directory identified by the given path from UFS into Alluxio. This does
   * not actually require looking at the UFS path.
   * It is a no-op if the directory exists and is persisted.
   *
   * @param inodePath the path for which metadata should be loaded
   * @param options the load metadata options
   * @param journalContext the journal context
   * @throws InvalidPathException if invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */

  private void loadDirectoryMetadataAndJournal(LockedInodePath inodePath,
      LoadMetadataOptions options, JournalContext journalContext)
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
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();
    String ufsOwner = ufs.getOwner(ufsUri.toString());
    String ufsGroup = ufs.getGroup(ufsUri.toString());
    short ufsMode = ufs.getMode(ufsUri.toString());
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryOptions =
        createDirectoryOptions.setOwner(ufsOwner).setGroup(ufsGroup).setMode(mode);

    try {
      createDirectoryAndJournal(inodePath, createDirectoryOptions, journalContext);
    } catch (FileAlreadyExistsException e) {
      // This should not happen.
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads metadata for the path if it is (non-existing || load direct children is set).
   *
   * @param inodePath the {@link LockedInodePath} to load the metadata for
   * @param options the load metadata options
   * @param journalContext the journal context
   */
  private void loadMetadataIfNotExistAndJournal(LockedInodePath inodePath,
      LoadMetadataOptions options, JournalContext journalContext) {
    boolean inodeExists = inodePath.fullPathExists();
    boolean loadDirectChildren = false;
    if (inodeExists) {
      try {
        Inode<?> inode = inodePath.getInode();
        loadDirectChildren = inode.isDirectory() && options.isLoadDirectChildren();
      } catch (FileDoesNotExistException e) {
        // This should never happen.
        throw new RuntimeException(e);
      }
    }
    if (!inodeExists || loadDirectChildren) {
      try {
        loadMetadataAndJournal(inodePath, options, journalContext);
      } catch (Exception e) {
        // NOTE, this may be expected when client tries to get info (e.g. exists()) for a file
        // existing neither in Alluxio nor UFS.
        LOG.debug("Failed to load metadata for path from UFS: {}", inodePath.getUri());
      }
    }
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  public void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException {
    Metrics.MOUNT_OPS.inc();
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree
            .lockInodePath(alluxioPath, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(alluxioPath);
      mountAndJournal(inodePath, ufsPath, options, journalContext);
      Metrics.PATHS_MOUNTED.inc();
    }
  }

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @param journalContext the journal context
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  private void mountAndJournal(LockedInodePath inodePath, AlluxioURI ufsPath, MountOptions options,
      JournalContext journalContext)
      throws InvalidPathException, FileAlreadyExistsException, FileDoesNotExistException,
      IOException, AccessControlException {
    // Check that the Alluxio Path does not exist
    if (inodePath.fullPathExists()) {
      // TODO(calvin): Add a test to validate this (ALLUXIO-1831)
      throw new InvalidPathException(
          ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(inodePath.getUri()));
    }

    mountInternal(inodePath, ufsPath, false /* not replayed */, options);
    boolean loadMetadataSucceeded = false;
    try {
      // This will create the directory at alluxioPath
      loadDirectoryMetadataAndJournal(inodePath,
          LoadMetadataOptions.defaults().setCreateAncestors(false), journalContext);
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
            .setUfsPath(ufsPath.toString()).setReadOnly(options.isReadOnly())
            .addAllProperties(protoProperties).setShared(options.isShared()).build();
    appendJournalEntry(JournalEntry.newBuilder().setAddMountPoint(addMountPoint).build(),
        journalContext);
  }

  /**
   * @param entry the entry to use
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  private void mountFromEntry(AddMountPointEntry entry)
      throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    AlluxioURI ufsURI = new AlluxioURI(entry.getUfsPath());
    try (LockedInodePath inodePath = mInodeTree
        .lockInodePath(alluxioURI, InodeTree.LockMode.WRITE)) {
      mountInternal(inodePath, ufsURI, true /* replayed */, new MountOptions(entry));
    }
  }

  /**
   * Updates the mount table with the specified mount point. The mount options may be updated during
   * this method.
   *
   * @param inodePath the Alluxio mount point
   * @param ufsPath the UFS endpoint to mount
   * @param replayed whether the operation is a result of replaying the journal
   * @param options the mount options (may be updated)
   * @throws FileAlreadyExistsException if the mount point already exists
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O exception occurs
   */
  private void mountInternal(LockedInodePath inodePath, AlluxioURI ufsPath, boolean replayed,
      MountOptions options) throws FileAlreadyExistsException, InvalidPathException, IOException {
    AlluxioURI alluxioPath = inodePath.getUri();

    if (!replayed) {
      // Check that the ufsPath exists and is a directory
      UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsPath.toString());
      ufs.setProperties(options.getProperties());
      if (!ufs.isDirectory(ufsPath.toString())) {
        throw new IOException(
            ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsPath.getPath()));
      }
      // Check that the alluxioPath we're creating doesn't shadow a path in the default UFS
      String defaultUfsPath = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      UnderFileSystem defaultUfs = UnderFileSystem.Factory.get(defaultUfsPath);
      String shadowPath = PathUtils.concatPath(defaultUfsPath, alluxioPath.getPath());
      if (defaultUfs.exists(shadowPath)) {
        throw new IOException(
            ExceptionMessage.MOUNT_PATH_SHADOWS_DEFAULT_UFS.getMessage(alluxioPath));
      }

      // Configure the ufs properties, and update the mount options with the configured properties.
      ufs.configureProperties();
      options.setProperties(ufs.getProperties());
    }

    // Add the mount point. This will only succeed if we are not mounting a prefix of an existing
    // mount and no existing mount is a prefix of this mount.
    mMountTable.add(alluxioPath, ufsPath, options);
  }

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to unmount, must be a mount point
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  public void unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException {
    Metrics.UNMOUNT_OPS.inc();
    // Unmount should lock the parent to remove the child inode.
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(alluxioPath, InodeTree.LockMode.WRITE_PARENT)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      unmountAndJournal(inodePath, journalContext);
      Metrics.PATHS_UNMOUNTED.inc();
    }
  }

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the Alluxio path to unmount, must be a mount point
   * @param journalContext the journal context
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws IOException if an I/O error occurs
   */
  private void unmountAndJournal(LockedInodePath inodePath, JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, IOException {
    if (unmountInternal(inodePath.getUri())) {
      Inode<?> inode = inodePath.getInode();
      // Use the internal delete API, setting {@code replayed} to true to prevent the delete
      // operations from being persisted in the UFS.
      long fileId = inode.getId();
      long opTimeMs = System.currentTimeMillis();
      deleteRecursiveInternal(inodePath, true /* replayed */, opTimeMs,
          DeleteOptions.defaults().setRecursive(true));
      DeleteFileEntry deleteFile =
          DeleteFileEntry.newBuilder().setId(fileId).setRecursive(true).setOpTimeMs(opTimeMs)
              .build();
      appendJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build(),
          journalContext);
      DeleteMountPointEntry deleteMountPoint =
          DeleteMountPointEntry.newBuilder().setAlluxioPath(inodePath.getUri().toString()).build();
      appendJournalEntry(JournalEntry.newBuilder().setDeleteMountPoint(deleteMountPoint).build(),
          journalContext);
    }
  }

  /**
   * @param entry the entry to use
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void unmountFromEntry(DeleteMountPointEntry entry) throws InvalidPathException {
    AlluxioURI alluxioURI = new AlluxioURI(entry.getAlluxioPath());
    if (!unmountInternal(alluxioURI)) {
      LOG.error("Failed to unmount {}", alluxioURI);
    }
  }

  /**
   * @param uri the Alluxio mount point to remove from the mount table
   * @return true if successful, false otherwise
   * @throws InvalidPathException if an invalid path is encountered
   */
  private boolean unmountInternal(AlluxioURI uri) throws InvalidPathException {
    return mMountTable.delete(uri);
  }

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid for the id of the file
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  public void resetFile(long fileId)
      throws UnexpectedAlluxioException, FileDoesNotExistException, InvalidPathException,
      AccessControlException {
    // TODO(yupeng) check the file is not persisted
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
      // free the file first
      InodeFile inodeFile = inodePath.getInodeFile();
      freeAndJournal(inodePath, FreeOptions.defaults().setForced(true), journalContext);
      inodeFile.reset();
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * This operation requires users to have {@link Mode.Bits#WRITE} permission on the path. In
   * addition, the client user must be a super user when setting the owner, and must be a super user
   * or the owner when setting the group or permission.
   *
   * @param path the path to set attribute for
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  public void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    Metrics.SET_ATTRIBUTE_OPS.inc();
    // for chown
    boolean rootRequired = options.getOwner() != null;
    // for chgrp, chmod
    boolean ownerRequired =
        (options.getGroup() != null) || (options.getMode() != Constants.INVALID_MODE);
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      mPermissionChecker.checkSetAttributePermission(inodePath, rootRequired, ownerRequired);
      setAttributeAndJournal(inodePath, rootRequired, ownerRequired, options, journalContext);
    }
  }

  /**
   * Sets the file attribute.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} to set attribute for
   * @param rootRequired indicates whether it requires to be the superuser
   * @param ownerRequired indicates whether it requires to be the owner of this path
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @param journalContext the journal context
   * @throws InvalidPathException if the given path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   */
  private void setAttributeAndJournal(LockedInodePath inodePath, boolean rootRequired,
      boolean ownerRequired, SetAttributeOptions options, JournalContext journalContext)
      throws InvalidPathException, FileDoesNotExistException, AccessControlException {
    Inode<?> targetInode = inodePath.getInode();
    long opTimeMs = System.currentTimeMillis();
    if (options.isRecursive() && targetInode.isDirectory()) {
      try (InodeLockList lockList = mInodeTree
          .lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
        List<Inode<?>> inodeChildren = lockList.getInodes();
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          try (LockedInodePath childPath = mInodeTree
              .lockFullInodePath(mInodeTree.getPath(inode), InodeTree.LockMode.READ)) {
            // TODO(gpang): a better way to check permissions
            mPermissionChecker.checkSetAttributePermission(childPath, rootRequired, ownerRequired);
          }
        }
        TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
        for (Inode<?> inode : inodeChildren) {
          // the path to inode for getPath should already be locked.
          tempInodePath.setDescendant(inode, mInodeTree.getPath(inode));
          List<Inode<?>> persistedInodes =
              setAttributeInternal(tempInodePath, false, opTimeMs, options);
          journalPersistedInodes(persistedInodes, journalContext);
          journalSetAttribute(tempInodePath, opTimeMs, options, journalContext);
        }
      }
    }
    List<Inode<?>> persistedInodes = setAttributeInternal(inodePath, false, opTimeMs, options);
    journalPersistedInodes(persistedInodes, journalContext);
    journalSetAttribute(inodePath, opTimeMs, options, journalContext);
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
    appendJournalEntry(JournalEntry.newBuilder().setSetAttribute(builder).build(), journalContext);
  }

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path of the file for persistence
   * @throws AlluxioException if scheduling fails
   */
  public void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException {
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree.lockFullInodePath(path, InodeTree.LockMode.WRITE)) {
      scheduleAsyncPersistenceAndJournal(inodePath, journalContext);
    }
    // NOTE: persistence is asynchronous so there is no guarantee the path will still exist
    mAsyncPersistHandler.scheduleAsyncPersistence(path);
  }

  /**
   * Schedules a file for async persistence.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the {@link LockedInodePath} of the file for persistence
   * @param journalContext the journal context
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceAndJournal(LockedInodePath inodePath,
      JournalContext journalContext) throws AlluxioException {
    long fileId = inodePath.getInode().getId();
    scheduleAsyncPersistenceInternal(inodePath);
    // write to journal
    AsyncPersistRequestEntry asyncPersistRequestEntry =
        AsyncPersistRequestEntry.newBuilder().setFileId(fileId).build();
    appendJournalEntry(
        JournalEntry.newBuilder().setAsyncPersistRequest(asyncPersistRequestEntry).build(),
        journalContext);
  }

  /**
   * @param inodePath the {@link LockedInodePath} of the file to persist
   * @throws AlluxioException if scheduling fails
   */
  private void scheduleAsyncPersistenceInternal(LockedInodePath inodePath) throws AlluxioException {
    inodePath.getInode().setPersistenceState(PersistenceState.TO_BE_PERSISTED);
  }

  /**
   * Instructs a worker to persist the files.
   * <p>
   * Needs {@link Mode.Bits#WRITE} permission on the list of files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
  public FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    for (long fileId : persistedFiles) {
      try {
        // Permission checking for each file is performed inside setAttribute
        setAttribute(getPath(fileId), SetAttributeOptions.defaults().setPersisted(true));
      } catch (FileDoesNotExistException | AccessControlException | InvalidPathException e) {
        LOG.error("Failed to set file {} as persisted, because {}", fileId, e);
      }
    }

    // get the files for the given worker to persist
    List<PersistFile> filesToPersist = mAsyncPersistHandler.pollFilesToPersist(workerId);
    if (!filesToPersist.isEmpty()) {
      LOG.debug("Sent files {} to worker {} to persist", filesToPersist, workerId);
    }
    FileSystemCommandOptions options = new FileSystemCommandOptions();
    options.setPersistOptions(new PersistCommandOptions(filesToPersist));
    return new FileSystemCommand(CommandType.Persist, options);
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
      if (inode.getTtl() != ttl) {
        mTtlBuckets.remove(inode);
        inode.setTtl(ttl);
        mTtlBuckets.insert(inode);
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
        MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
        String ufsUri = resolution.getUri().toString();
        if (UnderFileSystemUtils.isObjectStorage(ufsUri)) {
          LOG.warn("setOwner/setMode is not supported to object storage UFS via Alluxio. " + "UFS: "
              + ufsUri + ". This has no effect on the underlying object.");
        } else {
          UnderFileSystem ufs = resolution.getUfs();
          if (ownerGroupChanged) {
            try {
              String owner = options.getOwner() != null ? options.getOwner() : inode.getOwner();
              String group = options.getGroup() != null ? options.getGroup() : inode.getGroup();
              ufs.setOwner(ufsUri, owner, group);
            } catch (IOException e) {
              throw new AccessControlException("Could not setOwner for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
          if (modeChanged) {
            try {
              ufs.setMode(ufsUri, options.getMode());
            } catch (IOException e) {
              throw new AccessControlException("Could not setMode for UFS file " + ufsUri
                  + " . Aborting the setAttribute operation in Alluxio.", e);
            }
          }
        }
      }
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
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE)) {
      setAttributeInternal(inodePath, true, entry.getOpTimeMs(), options);
      // Intentionally not journaling the persisted inodes from setAttributeInternal
    }
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public List<WorkerInfo> getWorkerInfoList() {
    return mBlockMaster.getWorkerInfoList();
  }

  /**
   * This class represents the executor for periodic inode ttl check.
   */
  private final class MasterInodeTtlCheckExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link MasterInodeTtlCheckExecutor}.
     */
    public MasterInodeTtlCheckExecutor() {}

    @Override
    public void heartbeat() {
      Set<TtlBucket> expiredBuckets = mTtlBuckets.getExpiredBuckets(System.currentTimeMillis());
      for (TtlBucket bucket : expiredBuckets) {
        for (Inode inode : bucket.getInodes()) {
          AlluxioURI path = null;
          try (LockedInodePath inodePath = mInodeTree
              .lockFullInodePath(inode.getId(), InodeTree.LockMode.READ)) {
            path = inodePath.getUri();
          } catch (Exception e) {
            LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                e.toString());
          }
          if (path != null) {
            try {
              TtlAction ttlAction = inode.getTtlAction();
              LOG.debug("Path {} TTL has expired, performing action {}", path.getPath(), ttlAction);
              switch (ttlAction) {
                case FREE:
                  // public free method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    free(path, FreeOptions.defaults().setForced(true).setRecursive(true));
                  } else {
                    free(path, FreeOptions.defaults().setForced(true));
                  }
                  // Reset state
                  inode.setTtl(Constants.NO_TTL);
                  inode.setTtlAction(TtlAction.DELETE);
                  mTtlBuckets.remove(inode);
                  break;
                case DELETE:// Default if not set is DELETE
                  // public delete method will lock the path, and check WRITE permission required at
                  // parent of file
                  if (inode.isDirectory()) {
                    delete(path, DeleteOptions.defaults().setRecursive(true));
                  } else {
                    delete(path, DeleteOptions.defaults().setRecursive(false));
                  }
                  break;
                default:
                  LOG.error("Unknown ttl action {}", ttlAction);
              }
            } catch (Exception e) {
              LOG.error("Exception trying to clean up {} for ttl check: {}", inode.toString(),
                  e.toString());
            }
          }
        }
      }
      mTtlBuckets.removeBuckets(expiredBuckets);
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Lost files periodic check.
   */
  private final class LostFilesDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostFilesDetectionHeartbeatExecutor}.
     */
    public LostFilesDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      for (long fileId : getLostFiles()) {
        // update the state
        try (LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(fileId, InodeTree.LockMode.WRITE)) {
          Inode<?> inode = inodePath.getInode();
          if (inode.getPersistenceState() != PersistenceState.PERSISTED) {
            inode.setPersistenceState(PersistenceState.LOST);
          }
        } catch (FileDoesNotExistException e) {
          LOG.error("Exception trying to get inode from inode tree: {}", e.toString());
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  /**
   * Class that contains metrics for FileSystemMaster.
   * This class is public because the counter names are referenced in
   * {@link alluxio.web.WebInterfaceMasterMetricsServlet}.
   */
  public static final class Metrics {
    private static final Counter DIRECTORIES_CREATED =
        MetricsSystem.masterCounter("DirectoriesCreated");
    private static final Counter FILE_BLOCK_INFOS_GOT =
        MetricsSystem.masterCounter("FileBlockInfosGot");
    private static final Counter FILE_INFOS_GOT = MetricsSystem.masterCounter("FileInfosGot");
    private static final Counter FILES_COMPLETED = MetricsSystem.masterCounter("FilesCompleted");
    private static final Counter FILES_CREATED = MetricsSystem.masterCounter("FilesCreated");
    private static final Counter FILES_FREED = MetricsSystem.masterCounter("FilesFreed");
    private static final Counter FILES_PERSISTED = MetricsSystem.masterCounter("FilesPersisted");
    private static final Counter NEW_BLOCKS_GOT = MetricsSystem.masterCounter("NewBlocksGot");
    private static final Counter PATHS_DELETED = MetricsSystem.masterCounter("PathsDeleted");
    private static final Counter PATHS_MOUNTED = MetricsSystem.masterCounter("PathsMounted");
    private static final Counter PATHS_RENAMED = MetricsSystem.masterCounter("PathsRenamed");
    private static final Counter PATHS_UNMOUNTED = MetricsSystem.masterCounter("PathsUnmounted");

    // TODO(peis): Increment the RPCs OPs at the place where we receive the RPCs.
    private static final Counter COMPLETE_FILE_OPS = MetricsSystem.masterCounter("CompleteFileOps");
    private static final Counter CREATE_DIRECTORIES_OPS =
        MetricsSystem.masterCounter("CreateDirectoryOps");
    private static final Counter CREATE_FILES_OPS = MetricsSystem.masterCounter("CreateFileOps");
    private static final Counter DELETE_PATHS_OPS = MetricsSystem.masterCounter("DeletePathOps");
    private static final Counter FREE_FILE_OPS = MetricsSystem.masterCounter("FreeFileOps");
    private static final Counter GET_FILE_BLOCK_INFO_OPS =
        MetricsSystem.masterCounter("GetFileBlockInfoOps");
    private static final Counter GET_FILE_INFO_OPS = MetricsSystem.masterCounter("GetFileInfoOps");
    private static final Counter GET_NEW_BLOCK_OPS = MetricsSystem.masterCounter("GetNewBlockOps");
    private static final Counter MOUNT_OPS = MetricsSystem.masterCounter("MountOps");
    private static final Counter RENAME_PATH_OPS = MetricsSystem.masterCounter("RenamePathOps");
    private static final Counter SET_ATTRIBUTE_OPS = MetricsSystem.masterCounter("SetAttributeOps");
    private static final Counter UNMOUNT_OPS = MetricsSystem.masterCounter("UnmountOps");

    public static final String FILES_PINNED = "FilesPinned";
    public static final String PATHS_TOTAL = "PathsTotal";
    public static final String UFS_CAPACITY_TOTAL = "UfsCapacityTotal";
    public static final String UFS_CAPACITY_USED = "UfsCapacityUsed";
    public static final String UFS_CAPACITY_FREE = "UfsCapacityFree";

    /**
     * Register some file system master related gauges.
     */
    private static void registerGauges(final FileSystemMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(FILES_PINNED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPinnedFiles();
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(PATHS_TOTAL),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getNumberOfPaths();
            }
          });

      final String ufsDataFolder = Configuration.get(PropertyKey.UNDERFS_ADDRESS);
      final UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsDataFolder);

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_TOTAL);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
                ret = ufs.getSpace(ufsDataFolder, UnderFileSystem.SpaceType.SPACE_USED);
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return ret;
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMasterMetricName(UFS_CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              long ret = 0L;
              try {
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
}
