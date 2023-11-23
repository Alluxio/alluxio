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
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.LoadMetadataPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.LoadMetadataContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeFile;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.MutableInodeFile;
import alluxio.master.file.meta.SyncCheck;
import alluxio.master.file.meta.SyncCheck.SyncResult;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.journal.FileSystemMergeJournalContext;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.MergeJournalContext;
import alluxio.master.journal.MetadataSyncMergeJournalContext;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableIterator;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.LogUtils;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

import com.codahale.metrics.Counter;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.annotation.Nullable;

/**
 * This class is responsible for maintaining the logic which surrounds syncing metadata between
 * Alluxio and its UFSes.
 *
 * This implementation uses a BFS-based approach to crawl the inode tree. In order to speed up
 * the sync process we use an {@link ExecutorService} which we submit inode paths to using
 * {@link #processSyncPath(AlluxioURI, RpcContext)}.
 * The processing of inode paths will discover new paths to
 * sync depending on the {@link #mDescendantType}. Syncing is finished when all submitted tasks
 * are completed and there are no new inodes left in the queue.
 *
 * Syncing inode metadata requires making calls to the UFS. This implementation will schedule UFS
 * RPCs with the {@link UfsStatusCache#prefetchChildren(AlluxioURI, MountTable)}. Then, once the
 * inode begins processing, it can retrieve the results. After processing, it can then remove its
 * {@link UfsStatus} from the cache. This strategy helps reduce memory pressure on the master
 * while performing a sync for a large tree. Additionally, by using a prefetch mechanism we can
 * concurrently process other inodes while waiting for UFS RPCs to complete.
 *
 * With regard to locking, this class expects to be able to take a write lock on any inode, and
 * then subsequently downgrades or unlocks after the sync is finished. Even though we use
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock}, because we concurrently process
 * inodes on separate threads, we cannot utilize the reentrant behavior. The implications of
 * that mean the caller of this class must not hold a write while calling {@link #sync()}.
 *
 * A user of this class is expected to create a new instance for each path that they would like
 * to process. This is because the Lock on the {@link #mRootScheme} may be changed after calling
 * {@link #sync()}.
 *
 * When a sync happens on a directory, only the sync timestamp of the directory itself will be
 * updated (including information if the sync was recursive) and not its children.
 * Then whenever a path checked it will also check its parent's sync time up to the root.
 * There are currently two reasons for this, the first is so that the sync cache will be
 * updated only on the parent directory level and not for every child synced meaning there will be
 * fewer entries in the cache. Second that updating the children times individually would require
 * a redesign because the sync paths are not tracked in a way currently where they know
 * when their children finish (apart from the root sync path).
 *
 * When checking if a child of the root sync path needs to be synced, the following
 * two items are considered:
 * 1. If a child directory does not need to be synced, then it will not be synced.
 * The parent will then update its sync time only to the oldest sync time of a child
 * that was not synced (or the current clock time if all children were synced).
 * 2. If a child file does not need to be synced (but its updated state has already
 * been loaded from the UFS due to the listing of the parent directory) then the
 * sync is still performed (because no additional UFS operations are needed,
 * unless ACL is enabled for the UFS, then an additional UFS call would be
 * needed so the sync is skipped and the time is calculated as in 1.).
 *
 * To go through an example (note I am assuming every path here is a directory).
 * If say the interval is 100s, and the last synced timestamps are:
 * /a 0
 * /a/b 10
 * /a/c 0
 * /a/d 0
 * /a/e 0
 * /a/f 0
 * Then the current timestamp is 100 and a sync will trigger with the sync for /a/b skipped.
 * Then the timestamps look like the following:
 * /a 10
 * /a/b 10
 * /a/c 0
 * /a/d 0
 * /a/e 0
 * /a/f 0
 *
 * Now if we do a sync at timestamp 110, a metadata sync for /a will be triggered again,
 * all children are synced. After the operation, the timestamp looks like
 * (i.e. all paths have a sync time of 110):
 * /a 110
 * /a/b 10
 * /a/c 0
 * /a/d 0
 * /a/e 0
 * /a/f 0
 *
 * Here is a second example:
 * If say the interval is 100s, the last synced timestamps are:
 * /a 0
 * /a/b 0
 * /a/c 0
 * /a/d 0
 * /a/e 0
 * /a/f 0
 * Now say at time 90 some children are synced individually.
 * Then the timestamps look like the following:
 * /a 0
 * /a/b 0
 * /a/c 90
 * /a/d 90
 * /a/e 90
 * /a/f 90
 *
 * and if we do a sync at timestamp 100, a sync will only happen on /a/b,
 * and /a will get updated to 90
 * /a 90
 * /a/b 0
 * /a/c 90
 * /a/d 90
 * /a/e 90
 * /a/f 90
 *
 * Note that we may consider different ways of deciding how to sync children
 * (see https://github.com/Alluxio/alluxio/pull/16081).
 */
public class InodeSyncStream {
  /**
   * Return status of a sync result.
   */
  public enum SyncStatus {
    OK,
    FAILED,
    NOT_NEEDED
  }

  private static final Logger LOG = LoggerFactory.getLogger(InodeSyncStream.class);

  private static final FileSystemMasterCommonPOptions NO_TTL_OPTION =
      FileSystemMasterCommonPOptions.newBuilder()
          .setTtl(-1)
          .build();

  /** The root path. Should be locked with a write lock. */
  private final LockingScheme mRootScheme;

  /** A {@link UfsSyncPathCache} maintained from the {@link DefaultFileSystemMaster}. */
  private final UfsSyncPathCache mUfsSyncPathCache;

  /** Object holding the {@link UfsStatus}es which may be required for syncing. */
  private final UfsStatusCache mStatusCache;

  /** Inode tree to lock new paths. */
  private final InodeTree mInodeTree;

  /** Determines how deep in the tree we need to load. */
  private final DescendantType mDescendantType;

  /** The {@link RpcContext} from the caller. */
  private final RpcContext mRpcContext;

  /** The inode store to look up children. */
  private final ReadOnlyInodeStore mInodeStore;

  /** The mount table for looking up the proper UFS client based on the Alluxio path. */
  private final MountTable mMountTable;

  /** The lock manager used to try acquiring the persisting lock. */
  private final InodeLockManager mInodeLockManager;

  /** The FS master creating this object. */
  private final DefaultFileSystemMaster mFsMaster;

  /** Set this to true to force a sync regardless of the UfsPathCache. */
  private final boolean mForceSync;

  /** The sync options on the RPC.  */
  private final FileSystemMasterCommonPOptions mSyncOptions;

  /** To determine if we should use the MergeJournalContext to merge journals. */
  private final boolean mUseFileSystemMergeJournalContext = Configuration.getBoolean(
      PropertyKey.MASTER_FILE_SYSTEM_MERGE_INODE_JOURNALS
  );

  /** To determine whether we should only let the UFS sync happen once
   * for the concurrent metadata sync requests syncing the same directory.
   */
  private final boolean mDedupConcurrentSync = Configuration.getBoolean(
      PropertyKey.MASTER_METADATA_CONCURRENT_SYNC_DEDUP
  );
  private static final MetadataSyncLockManager SYNC_METADATA_LOCK_MANAGER =
      new MetadataSyncLockManager();

  /** Whether to only read+create metadata from the UFS, or to update metadata as well. */
  private final boolean mLoadOnly;

  /** Deque used to keep track of paths that still need to be synced. */
  private final ConcurrentLinkedDeque<AlluxioURI> mPendingPaths;

  /** The traversal order of {@link #mPendingPaths}. */
  private final MetadataSyncTraversalOrder mTraverseType;

  /** Queue of paths that have been submitted to the executor. */
  private final Queue<Future<SyncResult>> mSyncPathJobs;

  /** The executor enabling concurrent processing. */
  private final ExecutorService mMetadataSyncService;

  /** The interval of time passed (in ms) to require a new sync. */
  private final long mSyncInterval;

  /** The maximum number of concurrent paths that can be syncing at any moment. */
  private final int mConcurrencyLevel =
      Configuration.getInt(PropertyKey.MASTER_METADATA_SYNC_CONCURRENCY_LEVEL);

  private final boolean mGetDirectoryStatusSkipLoadingChildren =
      Configuration.getBoolean(
          PropertyKey.MASTER_METADATA_SYNC_GET_DIRECTORY_STATUS_SKIP_LOADING_CHILDREN);

  private final FileSystemMasterAuditContext mAuditContext;
  private final Function<LockedInodePath, Inode> mAuditContextSrcInodeFunc;

  private final Clock mClock;

  /**
   * Create a new instance of {@link InodeSyncStream}.
   *
   * The root path should be already locked with {@link LockPattern#WRITE_EDGE} unless the user is
   * only planning on loading metadata. The desired pattern should always be
   * {@link LockPattern#READ}.
   *
   * It is an error to initiate sync without a WRITE_EDGE lock when loadOnly is {@code false}.
   * If loadOnly is set to {@code true}, then the root path may have a read lock.
   *
   * @param rootPath The root path to begin syncing
   * @param fsMaster the {@link FileSystemMaster} calling this method
   * @param syncPathCache the {@link UfsSyncPathCache} for the given path
   * @param rpcContext the caller's {@link RpcContext}
   * @param descendantType determines the number of descendant inodes to sync
   * @param options the RPC's {@link FileSystemMasterCommonPOptions}
   * @param auditContext the audit context to use when loading
   * @param auditContextSrcInodeFunc the inode to set as the audit context source
   * @param forceSync whether to sync inode metadata no matter what
   * @param loadOnly whether to only load new metadata, rather than update existing metadata
   * @param loadAlways whether to always load new metadata from the ufs, even if a file or
   *                   directory has been previous found to not exist
   */
  public InodeSyncStream(LockingScheme rootPath, DefaultFileSystemMaster fsMaster,
      UfsSyncPathCache syncPathCache,
      RpcContext rpcContext, DescendantType descendantType, FileSystemMasterCommonPOptions options,
      @Nullable FileSystemMasterAuditContext auditContext,
      @Nullable Function<LockedInodePath, Inode> auditContextSrcInodeFunc,
      boolean forceSync, boolean loadOnly, boolean loadAlways)
  {
    mPendingPaths = new ConcurrentLinkedDeque<>();
    mTraverseType = Configuration.getEnum(PropertyKey.MASTER_METADATA_SYNC_TRAVERSAL_ORDER,
        MetadataSyncTraversalOrder.class);
    mDescendantType = descendantType;
    mRpcContext = rpcContext;
    mMetadataSyncService = fsMaster.mSyncMetadataExecutorIns;
    mClock = fsMaster.mClock;
    mForceSync = forceSync;
    mRootScheme = rootPath;
    mSyncOptions = options;
    mLoadOnly = loadOnly;
    mSyncPathJobs = new LinkedList<>();
    mFsMaster = fsMaster;
    mInodeLockManager = fsMaster.getInodeLockManager();
    mInodeStore = fsMaster.getInodeStore();
    mInodeTree = fsMaster.getInodeTree();
    mMountTable = fsMaster.getMountTable();
    mUfsSyncPathCache = syncPathCache;
    mAuditContext = auditContext;
    mAuditContextSrcInodeFunc = auditContextSrcInodeFunc;
    mSyncInterval = options.hasSyncIntervalMs() ? options.getSyncIntervalMs() :
        Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL);
    // If an absent cache entry was more recent than this value, then it is valid for this sync
    long validCacheTime;
    if (loadOnly) {
      if (loadAlways) {
        validCacheTime = UfsAbsentPathCache.NEVER;
      } else {
        validCacheTime = UfsAbsentPathCache.ALWAYS;
      }
    } else {
      validCacheTime = mClock.millis() - mSyncInterval;
    }
    mStatusCache = new UfsStatusCache(fsMaster.mSyncPrefetchExecutorIns,
        fsMaster.getAbsentPathCache(), validCacheTime);
    // Maintain a global counter of active sync streams
    DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_COUNT.inc();
  }

  /**
   * Create a new instance of {@link InodeSyncStream} without any audit or permission checks.
   *
   * @param rootScheme The root path to begin syncing
   * @param fsMaster the {@link FileSystemMaster} calling this method
   * @param syncPathCache the {@link UfsSyncPathCache} for this path
   * @param rpcContext the caller's {@link RpcContext}
   * @param descendantType determines the number of descendant inodes to sync
   * @param options the RPC's {@link FileSystemMasterCommonPOptions}
   * @param forceSync whether to sync inode metadata no matter what
   * @param loadOnly whether to only load new metadata, rather than update existing metadata
   * @param loadAlways whether to always load new metadata from the ufs, even if a file or
   *                   directory has been previous found to not exist
   */
  public InodeSyncStream(LockingScheme rootScheme, DefaultFileSystemMaster fsMaster,
      UfsSyncPathCache syncPathCache,
      RpcContext rpcContext, DescendantType descendantType, FileSystemMasterCommonPOptions options,
      boolean forceSync, boolean loadOnly, boolean loadAlways)
  {
    this(rootScheme, fsMaster, syncPathCache, rpcContext, descendantType, options, null, null,
        forceSync, loadOnly, loadAlways);
  }

  /**
   * Sync the metadata according the root path the stream was created with.
   * [WARNING]:
   * To avoid deadlock, please do not obtain any inode path lock before calling this method.
   *
   * @return SyncStatus object
   */
  public SyncStatus sync() throws AccessControlException, InvalidPathException {
    LOG.debug("Running InodeSyncStream on path {}, with status {}, and force sync {}",
        mRootScheme.getPath(), mRootScheme.shouldSync(), mForceSync);
    if (!mRootScheme.shouldSync().isShouldSync() && !mForceSync) {
      DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SKIPPED.inc();
      return SyncStatus.NOT_NEEDED;
    }
    if (!mDedupConcurrentSync) {
      return syncInternal();
    }
    try (MetadataSyncLockManager.MetadataSyncPathList ignored = SYNC_METADATA_LOCK_MANAGER.lockPath(
        mRootScheme.getPath())) {
      mRpcContext.throwIfCancelled();
      return syncInternal();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SyncStatus syncInternal() throws
      AccessControlException, InvalidPathException {
    // The high-level process for the syncing is:
    // 1. Given an Alluxio path, determine if it is not consistent with the corresponding UFS path.
    //     this means the UFS path does not exist, or has metadata which differs from Alluxio
    // 2. If only the metadata changed, update the inode with the new metadata
    // 3. If the path does not exist in the UFS, delete the inode in Alluxio
    // 4. If not deleted, load metadata from the UFS
    // 5. If a recursive sync, add children inodes to sync queue
    int syncPathCount = 0;
    int failedSyncPathCount = 0;
    int skippedSyncPathCount = 0;
    int stopNum = -1; // stop syncing when we've processed this many paths. -1 for infinite
    if (mDedupConcurrentSync && mRootScheme.shouldSync() != SyncCheck.SHOULD_SYNC) {
      /*
       * If a concurrent sync on the same path is successful after this sync had already
       * been initialized and that sync is successful, then there is no need to sync again.
       * This is done by checking is the last successful sync time for the path has
       * increased since this sync was started.
       * * e.g.
       * First assume the last sync time for path /aaa is 0
       * 1. [TS=100] the sync() method is called by thread A for path /aaa with sync
       *    interval 50, so a sync starts
       * 2. [TS=110] the sync() method is called by thread B for path /aaa,
       *    using syncInterval 100, so a sync starts, but
       *    thread B is blocked by the metadata sync lock,
       * 3. [TS=180] thread A finishes the metadata sync, update the SyncPathCache,
       *    setting the last sync timestamp to 100.
       * 4. [TS=182] thread B acquired the lock and can start sync
       * 5. [TS=182] since the sync time for the path was 0 when thread B started,
       *    and is now 100, thread B can skip the sync and return NOT_NEEDED.
       * Note that this still applies if A is to sync recursively path /aaa while B is to
       * sync path /aaa/bbb as the sync scope of A covers B's.
       */
      boolean shouldSkipSync =
          mUfsSyncPathCache.shouldSyncPath(mRootScheme.getPath(), mSyncInterval,
          mDescendantType).getLastSyncTime() > mRootScheme.shouldSync().getLastSyncTime();
      if (shouldSkipSync) {
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SKIPPED.inc();
        LOG.debug("Skipped sync on {} due to successful concurrent sync", mRootScheme.getPath());
        return SyncStatus.NOT_NEEDED;
      }
    }
    LOG.debug("Running InodeSyncStream on path {}", mRootScheme.getPath());
    long startTime = mUfsSyncPathCache.recordStartSync();
    boolean rootPathIsFile = false;

    RpcContext rpcContext = getMetadataSyncRpcContext();

    try (LockedInodePath path =
             mInodeTree.lockInodePath(mRootScheme, rpcContext.getJournalContext())) {
      if (mAuditContext != null && mAuditContextSrcInodeFunc != null) {
        mAuditContext.setSrcInode(mAuditContextSrcInodeFunc.apply(path));
      }
      syncInodeMetadata(path, rpcContext);
      syncPathCount++;
      if (mDescendantType == DescendantType.ONE) {
        // If descendantType is ONE, then we shouldn't process any more paths except for those
        // currently in the queue
        stopNum = mPendingPaths.size();
      } else if (mGetDirectoryStatusSkipLoadingChildren && mDescendantType == DescendantType.NONE) {
        // If descendantType is NONE, do not process any path in the queue after
        // the inode itself is loaded.
        stopNum = 0;
      }

      // process the sync result for the original path
      try {
        path.traverse();
        if (path.fullPathExists()) {
          rootPathIsFile = !path.getInode().isDirectory();
        }
      } catch (InvalidPathException e) {
        updateMetrics(false, startTime, syncPathCount, failedSyncPathCount);
        throw new RuntimeException(e);
      }
    } catch (FileDoesNotExistException e) {
      LOG.warn("Failed to sync metadata on root path {} because it"
              + " does not exist on the UFS or in Alluxio", this);
      failedSyncPathCount++;
    } catch (BlockInfoException | FileAlreadyCompletedException
        | InterruptedException | InvalidFileSizeException
        | IOException e) {
      LogUtils.warnWithException(LOG, "Failed to sync metadata on root path {}",
          toString(), e);
      failedSyncPathCount++;
    } catch (InvalidPathException | AccessControlException e) {
      // Catch and re-throw just to update metrics before exit
      LogUtils.warnWithException(LOG, "Failed to sync metadata on root path {}",
          toString(), e);
      updateMetrics(false, startTime, syncPathCount, failedSyncPathCount);
      throw e;
    } finally {
      // regardless of the outcome, remove the UfsStatus for this path from the cache
      mStatusCache.remove(mRootScheme.getPath());
      // add the remaining journals into the async journal writer
      maybeFlushJournalToAsyncJournalWriter(rpcContext);
    }

    // For any children that skip syncing because of a recent sync time,
    // we will only update the root path to the oldest of these times
    Long childOldestSkippedSync = null;
    // Process any children after the root.
    while (!mPendingPaths.isEmpty() || !mSyncPathJobs.isEmpty()) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Metadata syncing was interrupted before completion; {}", this);
        break;
      }
      if (mRpcContext.isCancelled()) {
        LOG.warn("Metadata syncing was cancelled before completion; {}", this);
        break;
      }
      // There are still paths to process
      // First, remove any futures which have completed. Add to the sync path count if they sync'd
      // successfully
      while (true) {
        Future<SyncResult> job = mSyncPathJobs.peek();
        if (job == null || !job.isDone()) {
          break;
        }
        // remove the job because we know it is done.
        if (mSyncPathJobs.poll() != job) {
          updateMetrics(false, startTime, syncPathCount, failedSyncPathCount);
          throw new ConcurrentModificationException("Head of queue modified while executing");
        }
        // Update a global counter
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_ACTIVE_PATHS_TOTAL.dec();
        try {
          // we synced the path successfully
          // This shouldn't block because we checked job.isDone() earlier
          SyncResult result = job.get();
          if (!result.isResultValid()) {
            failedSyncPathCount++;
          } else if (result.wasSyncPerformed()) {
            syncPathCount++;
          } else {
            skippedSyncPathCount++;
          }
          if (result.isResultValid() && !result.wasSyncPerformed()) {
            childOldestSkippedSync = childOldestSkippedSync == null ? result.getLastSyncTime()
                : Math.min(childOldestSkippedSync, result.getLastSyncTime());
          }
        } catch (InterruptedException | ExecutionException e) {
          failedSyncPathCount++;
          LogUtils.warnWithException(
              LOG, "metadata sync failed while polling for finished paths; {}",
              toString(), e);
          if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }

      // When using descendant type of ONE, we need to stop prematurely.
      if (stopNum != -1 && (syncPathCount + failedSyncPathCount + skippedSyncPathCount) > stopNum) {
        break;
      }

      // We can submit up to ( max_concurrency - <jobs queue size>) jobs back into the queue
      int submissions = mConcurrencyLevel - mSyncPathJobs.size();
      for (int i = 0; i < submissions; i++) {
        AlluxioURI path = pollItem();
        if (path == null) {
          // no paths left to sync
          break;
        }
        RpcContext rpcContextForSyncPath = getMetadataSyncRpcContext();
        Future<SyncResult> job =
            mMetadataSyncService.submit(() -> processSyncPath(path, rpcContextForSyncPath));
        mSyncPathJobs.offer(job);
        // Update global counters for all sync streams
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_PENDING_PATHS_TOTAL.dec();
        DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_ACTIVE_PATHS_TOTAL.inc();
      }
      // After submitting all jobs wait for the job at the head of the queue to finish.
      Future<SyncResult> oldestJob = mSyncPathJobs.peek();
      if (oldestJob == null) { // There might not be any jobs, restart the loop.
        continue;
      }
      try {
        oldestJob.get(); // block until the oldest job finished.
      } catch (InterruptedException | ExecutionException e) {
        LogUtils.warnWithException(
                LOG, "Exception while waiting for oldest metadata sync job to finish: {}",
                toString(), e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }

    boolean success = syncPathCount > 0;
    if (Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_REPORT_FAILURE)) {
      // There should not be any failed or outstanding jobs
      success = (failedSyncPathCount == 0) && mSyncPathJobs.isEmpty() && mPendingPaths.isEmpty();
    }
    if (success) {
      // update the sync path cache for the root of the sync
      // TODO(gpang): Do we need special handling for failures and thread interrupts?
      mUfsSyncPathCache.notifySyncedPath(mRootScheme.getPath(), mDescendantType,
          startTime, childOldestSkippedSync, rootPathIsFile);
    }
    mStatusCache.cancelAllPrefetch();
    mSyncPathJobs.forEach(f -> f.cancel(true));
    if (!mPendingPaths.isEmpty() || !mSyncPathJobs.isEmpty()) {
      DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_CANCEL.inc(
          mPendingPaths.size() + mSyncPathJobs.size());
    }
    if (!mSyncPathJobs.isEmpty()) {
      DefaultFileSystemMaster.Metrics
          .INODE_SYNC_STREAM_ACTIVE_PATHS_TOTAL.dec(mSyncPathJobs.size());
    }
    if (!mPendingPaths.isEmpty()) {
      DefaultFileSystemMaster.Metrics
          .INODE_SYNC_STREAM_PENDING_PATHS_TOTAL.dec(mPendingPaths.size());
    }

    maybeFlushJournalToAsyncJournalWriter(rpcContext);

    // Update metrics at the end of operation
    updateMetrics(success, startTime, syncPathCount, failedSyncPathCount);
    return success ? SyncStatus.OK : SyncStatus.FAILED;
  }

  private void updateMetrics(boolean success, long startTime,
      int successPathCount, int failedPathCount) {
    long duration = mClock.millis() - startTime;
    DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_TIME_MS.inc(duration);
    if (success) {
      DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SUCCESS.inc();
    } else {
      DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_FAIL.inc();
    }
    DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_SUCCESS.inc(successPathCount);
    DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_SYNC_PATHS_FAIL.inc(failedPathCount);
    if (LOG.isDebugEnabled()) {
      LOG.debug("synced {} paths ({} success, {} failed) in {} ms on {}",
          successPathCount + failedPathCount, successPathCount, failedPathCount,
          duration, mRootScheme);
    }
  }

  /**
   * Process a path to sync.
   *
   * This can update metadata for the inode, delete the inode, and/or queue any children that should
   * be synced as well.
   *
   * @param path The path to sync
   * @return true if this path was synced
   */
  private SyncResult processSyncPath(AlluxioURI path, RpcContext rpcContext)
      throws InvalidPathException {
    try {
      return processSyncPathInternal(path, rpcContext);
    } finally {
      maybeFlushJournalToAsyncJournalWriter(rpcContext);
    }
  }

  private SyncResult processSyncPathInternal(AlluxioURI path, RpcContext rpcContext)
      throws InvalidPathException {
    if (path == null) {
      return SyncResult.INVALID_RESULT;
    }

    // if we have already loaded the path from the UFS, and the path
    // is not a directory and ACL is disabled, then we will always finish the sync
    // (even if it is not needed) since we already have all the data we need
    boolean forceSync = !mFsMaster.isAclEnabled() && mStatusCache.hasStatus(path).map(
        ufsStatus -> !ufsStatus.isDirectory()).orElse(false);

    LockingScheme scheme;
    // forceSync is true means listStatus already prefetched metadata of children,
    // update metadata for such cases
    if (mForceSync || forceSync) {
      scheme = new LockingScheme(path, LockPattern.READ, true);
    } else {
      scheme = new LockingScheme(path, LockPattern.READ, mSyncOptions,
          mUfsSyncPathCache, mDescendantType);
    }

    if (!scheme.shouldSync().isShouldSync() && !mForceSync) {
      return scheme.shouldSync().skippedSync();
    }
    try (LockedInodePath inodePath =
             mInodeTree.tryLockInodePath(scheme, rpcContext.getJournalContext())) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Thread syncing {} was interrupted before completion", inodePath.getUri());
        return SyncResult.INVALID_RESULT;
      }
      syncInodeMetadata(inodePath, rpcContext);
      return scheme.shouldSync().syncSuccess();
    } catch (AccessControlException | BlockInfoException | FileAlreadyCompletedException
        | FileDoesNotExistException | InterruptedException | InvalidFileSizeException
        | InvalidPathException | IOException e) {
      LogUtils.warnWithException(LOG, "Failed to process sync path: {}", path, e);
    } finally {
      // regardless of the outcome, remove the UfsStatus for this path from the cache
      mStatusCache.remove(path);
    }
    return SyncResult.INVALID_RESULT;
  }

  private void syncInodeMetadata(LockedInodePath inodePath, RpcContext rpcContext)
      throws InvalidPathException, AccessControlException, IOException, FileDoesNotExistException,
      FileAlreadyCompletedException, InvalidFileSizeException, BlockInfoException,
      InterruptedException {
    if (!inodePath.fullPathExists()) {
      loadMetadataForPath(inodePath, rpcContext);
      // skip the load metadata step in the sync if it has been just loaded
      syncExistingInodeMetadata(inodePath, rpcContext, true);
    } else {
      syncExistingInodeMetadata(inodePath, rpcContext, false);
    }
  }

  private Object getFromUfs(Callable<Object> task, RpcContext rpcContext)
      throws InterruptedException {
    final Future<Object> future = mFsMaster.mSyncPrefetchExecutorIns.submit(task);
    DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_OPS_COUNT.inc();
    while (true) {
      try {
        Object j = future.get(1, TimeUnit.SECONDS);
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_SUCCESS.inc();
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_PATHS.inc();
        return j;
      } catch (TimeoutException e) {
        rpcContext.throwIfCancelled();
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_RETRIES.inc();
      } catch (ExecutionException e) {
        LogUtils.warnWithException(LOG, "Failed to get result for prefetch job", e);
        DefaultFileSystemMaster.Metrics.METADATA_SYNC_PREFETCH_FAIL.inc();
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Sync inode metadata with the UFS state.
   *
   * This method expects the {@code inodePath} to already exist in the inode tree.
   */
  private void syncExistingInodeMetadata(
      LockedInodePath inodePath, RpcContext rpcContext, boolean skipLoad)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException,
      InterruptedException {
    if (inodePath.getLockPattern() != LockPattern.WRITE_EDGE && !mLoadOnly) {
      throw new RuntimeException(String.format(
          "syncExistingInodeMetadata was called on d%s when only locked with %s. Load metadata"
          + " only was not specified.", inodePath.getUri(), inodePath.getLockPattern()));
    }

    // Set to true if the given inode was deleted.
    boolean deletedInode = false;
    // whether we need to load metadata for the current path
    boolean loadMetadata = mLoadOnly;
    LOG.trace("Syncing inode metadata {}", inodePath.getUri());

    // The requested path already exists in Alluxio.
    Inode inode = inodePath.getInode();
    // initialize sync children to true if it is a listStatus call on a directory
    boolean syncChildren = inode.isDirectory()
        && !mDescendantType.equals(DescendantType.NONE);

    // if the lock pattern is WRITE_EDGE, then we can sync (update or delete). Otherwise, if it is
    // we can only load metadata.

    if (inodePath.getLockPattern() == LockPattern.WRITE_EDGE && !mLoadOnly) {
      if (inode instanceof InodeFile && !inode.asFile().isCompleted()) {
        // Do not sync an incomplete file, since the UFS file is expected to not exist.
        return;
      }

      Optional<Scoped> persistingLock = mInodeLockManager.tryAcquirePersistingLock(inode.getId());
      if (!persistingLock.isPresent()) {
        // Do not sync a file in the process of being persisted, since the UFS file is being
        // written.
        return;
      }
      persistingLock.get().close();

      UfsStatus cachedStatus = null;
      boolean fileNotFound = false;
      try {
        cachedStatus = mStatusCache.getStatus(inodePath.getUri());
      } catch (FileNotFoundException e) {
        fileNotFound = true;
      }
      MountTable.Resolution resolution = mMountTable.resolve(inodePath.getUri());
      AlluxioURI ufsUri = resolution.getUri();
      try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        Fingerprint ufsFpParsed;
        // When the status is not cached and it was not due to file not found, we retry
        if (fileNotFound) {
          ufsFpParsed = Fingerprint.INVALID_FINGERPRINT;
        } else if (cachedStatus == null) {
          ufsFpParsed =
              (Fingerprint) getFromUfs(
                  () -> ufs.getParsedFingerprint(ufsUri.toString()), rpcContext);
          mMountTable.getUfsSyncMetric(resolution.getMountId()).inc();
        } else {
          // When the status is cached
          if (!mFsMaster.isAclEnabled()) {
            ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus);
          } else {
            Pair<AccessControlList, DefaultAccessControlList> aclPair =
                (Pair<AccessControlList, DefaultAccessControlList>)
                    getFromUfs(() -> ufs.getAclPair(ufsUri.toString()), rpcContext);
            mMountTable.getUfsSyncMetric(resolution.getMountId()).inc();
            if (aclPair == null || aclPair.getFirst() == null
                || !aclPair.getFirst().hasExtended()) {
              ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus);
            } else {
              ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus, null,
                  aclPair.getFirst());
            }
          }
        }
        boolean containsMountPoint = mMountTable.containsMountPoint(inodePath.getUri(), true);

        UfsSyncUtils.SyncPlan syncPlan =
            UfsSyncUtils.computeSyncPlan(inode, ufsFpParsed, containsMountPoint);
        if (!syncPlan.toUpdateMetaData() && !syncPlan.toDelete()) {
          DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_NO_CHANGE.inc();
        }

        if (syncPlan.toUpdateMetaData()) {
          // UpdateMetadata is used when a file or a directory only had metadata change.
          // It works by calling SetAttributeInternal on the inodePath.
          if (ufsFpParsed != null && ufsFpParsed.isValid()) {
            short mode = Short.parseShort(ufsFpParsed.getTag(Fingerprint.Tag.MODE));
            long opTimeMs = mClock.millis();
            SetAttributePOptions.Builder builder = SetAttributePOptions.newBuilder()
                .setMode(new Mode(mode).toProto());
            String owner = ufsFpParsed.getTag(Fingerprint.Tag.OWNER);
            if (!owner.equals(Fingerprint.UNDERSCORE)) {
              // Only set owner if not empty
              builder.setOwner(owner);
            }
            String group = ufsFpParsed.getTag(Fingerprint.Tag.GROUP);
            if (!group.equals(Fingerprint.UNDERSCORE)) {
              // Only set group if not empty
              builder.setGroup(group);
            }
            SetAttributeContext ctx = SetAttributeContext.mergeFrom(builder)
                .setUfsFingerprint(ufsFpParsed.serialize()).setMetadataLoad(true);
            mFsMaster.setAttributeSingleFile(rpcContext, inodePath, false, opTimeMs, ctx);
          }
        }

        if (syncPlan.toDelete()) {
          deletedInode = true;
          try {
            // The options for deleting.
            DeleteContext syncDeleteContext = DeleteContext.mergeFrom(
                DeletePOptions.newBuilder()
                .setRecursive(true)
                .setAlluxioOnly(true)
                .setUnchecked(true))
                .setMetadataLoad(true);
            mFsMaster.deleteInternal(rpcContext, inodePath, syncDeleteContext, true);
          } catch (DirectoryNotEmptyException | IOException e) {
            // Should not happen, since it is an unchecked delete.
            LOG.error("Unexpected error for unchecked delete.", e);
          }
        }

        if (syncPlan.toLoadMetadata()) {
          loadMetadata = true;
        }

        syncChildren = syncPlan.toSyncChildren();
      }
    }

    // sync plan does not know about the root of the sync
    // if DescendantType.ONE we only sync children if we are syncing root of this stream
    if (mDescendantType == DescendantType.ONE) {
      syncChildren =
          syncChildren && mRootScheme.getPath().equals(inodePath.getUri());
    } else if (mDescendantType == DescendantType.NONE && mGetDirectoryStatusSkipLoadingChildren) {
      syncChildren = false;
    }

    int childCount = inode.isDirectory() ? (int) inode.asDirectory().getChildCount() : 0;
    Map<String, Inode> inodeChildren = new HashMap<>(childCount);
    if (syncChildren) {
      // maps children name to inode
      try (CloseableIterator<? extends Inode> children = mInodeStore
          .getChildren(inode.asDirectory())) {
        children.forEachRemaining(child -> inodeChildren.put(child.getName(), child));
      }

      // Fetch and populate children into the cache
      mStatusCache.prefetchChildren(inodePath.getUri(), mMountTable);
      Collection<UfsStatus> listStatus = mStatusCache
          .fetchChildrenIfAbsent(rpcContext, inodePath.getUri(), mMountTable);
      // Iterate over UFS listings and process UFS children.
      if (listStatus != null) {
        for (UfsStatus ufsChildStatus : listStatus) {
          if (!inodeChildren.containsKey(ufsChildStatus.getName()) && !PathUtils
              .isTemporaryFileName(ufsChildStatus.getName())) {
            // Ufs child exists, but Alluxio child does not. Must load metadata.
            loadMetadata = true;
            break;
          }
        }
      }
    }
    // If the inode was deleted in the previous sync step, we need to remove the inode from the
    // locked path
    if (deletedInode) {
      inodePath.removeLastInode();
    }

    // load metadata if necessary.
    if (loadMetadata && !skipLoad) {
      loadMetadataForPath(inodePath, rpcContext);
    }

    boolean prefetchChildrenUfsStatus = Configuration.getBoolean(
        PropertyKey.MASTER_METADATA_SYNC_UFS_PREFETCH_ENABLED);

    if (syncChildren) {
      // Iterate over Alluxio children and process persisted children.
      try (CloseableIterator<? extends Inode> children
               = mInodeStore.getChildren(inode.asDirectory())) {
        children.forEachRemaining(childInode -> {
          // If we are only loading non-existing metadata, then don't process any child which
          // was already in the tree, unless it is a directory, in which case, we might need to load
          // its children.
          if (mLoadOnly && inodeChildren.containsKey(childInode.getName()) && childInode.isFile()) {
            return;
          }
          // If we're performing a recursive sync, add each child of our current Inode to the queue
          AlluxioURI child = inodePath.getUri().joinUnsafe(childInode.getName());
          mPendingPaths.add(child);
          // Update a global counter for all sync streams
          DefaultFileSystemMaster.Metrics.INODE_SYNC_STREAM_PENDING_PATHS_TOTAL.inc();

          if (prefetchChildrenUfsStatus) {
            // This asynchronously schedules a job to pre-fetch the statuses into the cache.
            if (childInode.isDirectory() && mDescendantType == DescendantType.ALL) {
              mStatusCache.prefetchChildren(child, mMountTable);
            }
          }
        });
      }
    }
  }

  private void loadMetadataForPath(LockedInodePath inodePath, RpcContext rpcContext)
      throws InvalidPathException, AccessControlException, IOException, FileDoesNotExistException,
      FileAlreadyCompletedException, InvalidFileSizeException, BlockInfoException {
    UfsStatus status = mStatusCache.fetchStatusIfAbsent(inodePath.getUri(), mMountTable);
    DescendantType descendantType = mDescendantType;
    // If loadMetadata is only for one level, and the path is not the root of the loadMetadata,
    // do not load the subdirectory
    if (descendantType.equals(DescendantType.ONE)
        && !inodePath.getUri().equals(mRootScheme.getPath())) {
      descendantType = DescendantType.NONE;
    }

    FileSystemMasterCommonPOptions option = NO_TTL_OPTION;
    if (!Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_IGNORE_TTL)) {
      option = mSyncOptions;
    }

    LoadMetadataContext ctx = LoadMetadataContext.mergeFrom(
        LoadMetadataPOptions.newBuilder()
            .setCommonOptions(option)
            .setCreateAncestors(true)
            .setLoadDescendantType(GrpcUtils.toProto(descendantType)))
        .setUfsStatus(status);
    loadMetadata(inodePath, rpcContext, ctx);
  }

  /**
  * This method creates inodes containing the metadata from the UFS. The {@link UfsStatus} object
  * must be set in the {@link LoadMetadataContext} in order to successfully create the inodes.
  */
  private void loadMetadata(
      LockedInodePath inodePath, RpcContext rpcContext, LoadMetadataContext context)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    int failedSync = 0;
    try {
      if (context.getUfsStatus() == null) {
        // uri does not exist in ufs
        Inode inode = inodePath.getInode();
        if (inode.isFile()) {
          throw new IllegalArgumentException(String.format(
              "load metadata cannot be called on a file if no ufs "
                  + "status is present in the context. %s", inodePath.getUri()));
        }

        mInodeTree.setDirectChildrenLoaded(rpcContext, inode.asDirectory());
        return;
      }

      if (context.getUfsStatus().isFile()) {
        loadFileMetadataInternal(rpcContext, inodePath, resolution, context, mFsMaster,
            mMountTable);
      } else {
        loadDirectoryMetadata(rpcContext, inodePath, context, mMountTable, mFsMaster);

        // now load all children if required
        LoadDescendantPType type = context.getOptions().getLoadDescendantType();
        if (type != LoadDescendantPType.NONE) {
          Collection<UfsStatus> children = mStatusCache.fetchChildrenIfAbsent(rpcContext,
              inodePath.getUri(), mMountTable);
          if (children == null) {
            LOG.debug("fetching children for {} returned null", inodePath.getUri());
            return;
          }
          for (UfsStatus childStatus : children) {
            if (PathUtils.isTemporaryFileName(childStatus.getName())) {
              continue;
            }
            AlluxioURI childURI = new AlluxioURI(PathUtils.concatPath(inodePath.getUri(),
                childStatus.getName()));
            if (mInodeTree.inodePathExists(childURI) && (childStatus.isFile()
                || context.getOptions().getLoadDescendantType() != LoadDescendantPType.ALL)) {
              // stop traversing if this is an existing file, or an existing directory without
              // loading all descendants.
              continue;
            }
            LoadMetadataContext loadMetadataContext =
                LoadMetadataContext.mergeFrom(LoadMetadataPOptions.newBuilder()
                    .setLoadDescendantType(LoadDescendantPType.NONE)
                    .setCommonOptions(context.getOptions().getCommonOptions())
                    .setCreateAncestors(false))
                .setUfsStatus(childStatus);
            try (LockedInodePath descendant = inodePath
                .lockDescendant(inodePath.getUri().joinUnsafe(childStatus.getName()),
                    LockPattern.READ)) {
              loadMetadata(descendant, rpcContext, loadMetadataContext);
            } catch (FileNotFoundException e) {
              LOG.debug("Failed to loadMetadata because file is not in ufs:"
                  + " inodePath={}, options={}.",
                  childURI, loadMetadataContext, e);
            } catch (BlockInfoException | FileAlreadyCompletedException
                | FileDoesNotExistException | InvalidFileSizeException
                | IOException e) {
              LOG.debug("Failed to loadMetadata because the ufs file or directory"
                  + " is {}, options={}.",
                  childStatus, loadMetadataContext, e);
              failedSync++;
            }
          }
          mInodeTree.setDirectChildrenLoaded(rpcContext, inodePath.getInode().asDirectory());
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.debug("Failed to loadMetadata: inodePath={}, context={}.", inodePath.getUri(), context,
          e);
      throw new IOException(e);
    }
    if (failedSync > 0) {
      throw new IOException(String.format("Failed to load metadata of %s files or directories "
          + "under %s", failedSync, path));
    }
  }

  /**
   * Return item according to different TraverseTypes.
   *
   * @return alluxio uri
   */
  private AlluxioURI pollItem() {
    if (mTraverseType == MetadataSyncTraversalOrder.DFS) {
      return mPendingPaths.pollLast();
    } else {
      return mPendingPaths.poll();
    }
  }

  /**
   * Merge inode entry with subsequent update inode and update inode file entries.
   *
   * @param entries list of journal entries
   * @return a list of compacted journal entries
   */
  public static List<Journal.JournalEntry> mergeCreateComplete(
      List<alluxio.proto.journal.Journal.JournalEntry> entries) {
    List<alluxio.proto.journal.Journal.JournalEntry> newEntries = new ArrayList<>(entries.size());
    // file id : index in the newEntries, InodeFileEntry
    Map<Long, Pair<Integer, MutableInodeFile>> fileEntryMap = new HashMap<>(entries.size());
    for (alluxio.proto.journal.Journal.JournalEntry oldEntry : entries) {
      if (oldEntry.hasInodeFile()) {
        // Use the old entry as a placeholder, to be replaced later
        newEntries.add(oldEntry);
        fileEntryMap.put(oldEntry.getInodeFile().getId(),
            new Pair<>(newEntries.size() - 1,
                MutableInodeFile.fromJournalEntry(oldEntry.getInodeFile())));
      } else if (oldEntry.hasUpdateInode()) {
        File.UpdateInodeEntry entry = oldEntry.getUpdateInode();
        if (fileEntryMap.get(entry.getId()) == null) {
          newEntries.add(oldEntry);
          continue;
        }
        MutableInodeFile inode = fileEntryMap.get(entry.getId()).getSecond();
        inode.updateFromEntry(entry);
      } else if (oldEntry.hasUpdateInodeFile()) {
        File.UpdateInodeFileEntry entry = oldEntry.getUpdateInodeFile();
        if (fileEntryMap.get(entry.getId()) == null) {
          newEntries.add(oldEntry);
          continue;
        }
        MutableInodeFile inode = fileEntryMap.get(entry.getId()).getSecond();
        inode.updateFromEntry(entry);
      } else {
        newEntries.add(oldEntry);
      }
    }
    for (Pair<Integer, MutableInodeFile> pair : fileEntryMap.values()) {
      // Replace the old entry placeholder with the new entry,
      // to create the file in the same place in the journal
      newEntries.set(pair.getFirst(),
          pair.getSecond().toJournalEntry());
    }
    return newEntries;
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
  void loadFileMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataContext context,
      DefaultFileSystemMaster fsMaster, MountTable mountTable)
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
      Counter accessMetric = mountTable.getUfsSyncMetric(resolution.getMountId());

      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingFileStatus(ufsUri.toString()));
        accessMetric.inc();
      }
      ufsLength = ((UfsFileStatus) context.getUfsStatus()).getContentLength();
      long blockSize = ((UfsFileStatus) context.getUfsStatus()).getBlockSize();
      ufsBlockSizeByte = blockSize != UfsFileStatus.UNKNOWN_BLOCK_SIZE
          ? blockSize : ufs.getBlockSizeByte(ufsUri.toString());

      if (fsMaster.isAclEnabled()) {
        Pair<AccessControlList, DefaultAccessControlList> aclPair
            = ufs.getAclPair(ufsUri.toString());
        accessMetric.inc();
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
    createFileContext.setWriteType(WriteType.THROUGH); // set as through since already in UFS
    createFileContext.setMetadataLoad(true, true);
    createFileContext.setOwner(context.getUfsStatus().getOwner());
    createFileContext.setGroup(context.getUfsStatus().getGroup());
    createFileContext.setXAttr(context.getUfsStatus().getXAttr());
    if (createFileContext.getXAttr() == null) {
      createFileContext.setXAttr(new HashMap<String, byte[]>());
    }
    createFileContext.getXAttr().put(Constants.ETAG_XATTR_KEY,
        ((UfsFileStatus) context.getUfsStatus()).getContentHash().getBytes());

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
    // If the journal context is a MetadataSyncMergeJournalContext, then the
    // journals will be taken care and merged by that context already and hence
    // there's no need to create a new MergeJournalContext.
    boolean shouldUseMetadataSyncMergeJournalContext =
        mUseFileSystemMergeJournalContext
            && rpcContext.getJournalContext() instanceof MetadataSyncMergeJournalContext;
    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite();
         JournalContext merger = shouldUseMetadataSyncMergeJournalContext
             ? NoopJournalContext.INSTANCE
             : new MergeJournalContext(rpcContext.getJournalContext(),
             writeLockedPath.getUri(),
             InodeSyncStream::mergeCreateComplete)
    ) {
      // We do not want to close this wrapRpcContext because it uses elements from another context
      RpcContext wrapRpcContext = shouldUseMetadataSyncMergeJournalContext
          ? rpcContext
          : new RpcContext(
              rpcContext.getBlockDeletionContext(), merger, rpcContext.getOperationContext());
      fsMaster.createFileInternal(wrapRpcContext, writeLockedPath, createFileContext, true);
      CompleteFileContext completeContext =
          CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(ufsLength))
              .setUfsStatus(context.getUfsStatus()).setMetadataLoad(true);
      if (ufsLastModified != null) {
        completeContext.setOperationTimeMs(ufsLastModified);
      }
      fsMaster.completeFileInternal(wrapRpcContext, writeLockedPath, completeContext);
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
  static void loadDirectoryMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context, MountTable mountTable, DefaultFileSystemMaster fsMaster)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException {
    // Return if the full path exists because the sync cares only about keeping up with the UFS
    // If the mount point target path exists, the later mount logic will complain.
    if (inodePath.fullPathExists()) {
      return;
    }
    MountTable.Resolution resolution = mountTable.resolve(inodePath.getUri());
    // create the actual metadata
    loadDirectoryMetadataInternal(rpcContext, mountTable, context, inodePath, resolution.getUri(),
        resolution.getMountId(), resolution.getUfsClient(), fsMaster,
        mountTable.isMountPoint(inodePath.getUri()), resolution.getShared());
  }

  /**
   * Loads metadata for a mount point. This method acquires a WRITE_EDGE lock on
   * the target mount path.
   */
  static void loadMountPointDirectoryMetadata(RpcContext rpcContext, LockedInodePath inodePath,
      LoadMetadataContext context, MountTable mountTable, long mountId, boolean isShared,
      AlluxioURI ufsUri, UfsManager.UfsClient ufsClient, DefaultFileSystemMaster fsMaster) throws
      FileDoesNotExistException, IOException, InvalidPathException, AccessControlException {
    if (inodePath.fullPathExists()) {
      return;
    }
    // create the actual metadata
    loadDirectoryMetadataInternal(rpcContext, mountTable, context, inodePath, ufsUri,
        mountId, ufsClient, fsMaster, true, isShared);
  }

  /**
   * Creates the actual inodes based on the given context and configs.
   */
  private static void loadDirectoryMetadataInternal(RpcContext rpcContext, MountTable mountTable,
      LoadMetadataContext context, LockedInodePath inodePath, AlluxioURI ufsUri, long mountId,
      UfsManager.UfsClient ufsClient, DefaultFileSystemMaster fsMaster, boolean isMountPoint,
      boolean isShared) throws FileDoesNotExistException, InvalidPathException,
      AccessControlException, IOException {
    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
    createDirectoryContext.getOptions()
        .setRecursive(context.getOptions().getCreateAncestors()).setAllowExists(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createDirectoryContext.setMountPoint(isMountPoint);
    createDirectoryContext.setMetadataLoad(true, true);
    createDirectoryContext.setWriteType(WriteType.THROUGH);

    AccessControlList acl = null;
    DefaultAccessControlList defaultAcl = null;
    try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      Counter accessMetric = mountTable.getUfsSyncMetric(mountId);
      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingDirectoryStatus(ufsUri.toString()));
        accessMetric.inc();
      }
      Pair<AccessControlList, DefaultAccessControlList> aclPair =
          ufs.getAclPair(ufsUri.toString());
      if (aclPair != null) {
        accessMetric.inc();
        acl = aclPair.getFirst();
        defaultAcl = aclPair.getSecond();
      }
    }
    String ufsOwner = context.getUfsStatus().getOwner();
    String ufsGroup = context.getUfsStatus().getGroup();
    short ufsMode = context.getUfsStatus().getMode();
    Long lastModifiedTime = context.getUfsStatus().getLastModifiedTime();
    Mode mode = new Mode(ufsMode);
    if (isShared) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryContext.getOptions().setMode(mode.toProto());
    createDirectoryContext.setOwner(ufsOwner).setGroup(ufsGroup)
        .setUfsStatus(context.getUfsStatus());
    createDirectoryContext.setXAttr(context.getUfsStatus().getXAttr());
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
      fsMaster.createDirectoryInternal(rpcContext, writeLockedPath, ufsClient, ufsUri,
          createDirectoryContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the directory before we got the write lock.
      // The directory already exists, so nothing needs to be loaded.
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  private void maybeFlushJournalToAsyncJournalWriter(RpcContext rpcContext) {
    if (mUseFileSystemMergeJournalContext
        && rpcContext.getJournalContext() instanceof MetadataSyncMergeJournalContext) {
      try {
        rpcContext.getJournalContext().flush();
      } catch (UnavailableException e) {
        // This should never happen because rpcContext is a MetadataSyncMergeJournalContext type
        // and only flush journal asynchronously
        throw new RuntimeException("Flush journal failed. This should never happen.");
      }
    }
  }

  protected RpcContext getMetadataSyncRpcContext() {
    JournalContext journalContext = mRpcContext.getJournalContext();
    if (mUseFileSystemMergeJournalContext
        && journalContext instanceof FileSystemMergeJournalContext) {
      return new RpcContext(
          mRpcContext.getBlockDeletionContext(),
          new MetadataSyncMergeJournalContext(
              ((FileSystemMergeJournalContext) journalContext).getUnderlyingJournalContext(),
              new FileSystemJournalEntryMerger()),
          mRpcContext.getOperationContext());
    }
    return mRpcContext;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("rootPath", mRootScheme)
        .add("descendantType", mDescendantType)
        .add("commonOptions", mSyncOptions)
        .add("forceSync", mForceSync)
        .toString();
  }
}
