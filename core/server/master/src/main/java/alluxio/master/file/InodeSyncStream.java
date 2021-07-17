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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
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
import alluxio.master.file.contexts.GetStatusContext;
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
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.journal.MergeJournalContext;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.proto.journal.File;
import alluxio.proto.journal.Journal;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.AccessControlList;
import alluxio.security.authorization.DefaultAccessControlList;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UfsStatusCache;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.LogUtils;
import alluxio.util.interfaces.Scoped;
import alluxio.util.io.PathUtils;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
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
import java.util.concurrent.ConcurrentLinkedQueue;
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
 * {@link #processSyncPath(AlluxioURI)}. The processing of inode paths will discover new paths to
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
 * With regards to locking, this class expects to be able to take a write lock on any inode, and
 * then subsequently downgrades or unlocks after the sync is finished. Even though we use
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock}, because we concurrently process
 * inodes on separate threads, we cannot utilize the reetrnant behavior. The implications of
 * that mean the caller of this class must not hold a write while calling {@link #sync()}.
 *
 * A user of this class is expected to create a new instance for each path that they would like
 * to process. This is because the Lock on the {@link #mRootScheme} may be changed after calling
 * {@link #sync()}.
 *
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

  /**
   * Whether the caller is {@link FileSystemMaster#getFileInfo(AlluxioURI, GetStatusContext)}.
   * This is used for the {@link #mUfsSyncPathCache}.
   */
  private final boolean mIsGetFileInfo;

  /** Whether to only read+create metadata from the UFS, or to update metadata as well. */
  private final boolean mLoadOnly;

  /** Queue used to keep track of paths that still need to be synced. */
  private final ConcurrentLinkedQueue<AlluxioURI> mPendingPaths;

  /** Queue of paths that have been submitted to the executor. */
  private final Queue<Future<Boolean>> mSyncPathJobs;

  /** The executor enabling concurrent processing. */
  private final ExecutorService mMetadataSyncService;

  /** The maximum number of concurrent paths that can be syncing at any moment. */
  private final int mConcurrencyLevel =
      ServerConfiguration.getInt(PropertyKey.MASTER_METADATA_SYNC_CONCURRENCY_LEVEL);

  private final FileSystemMasterAuditContext mAuditContext;
  private final Function<LockedInodePath, Inode> mAuditContextSrcInodeFunc;
  private final DefaultFileSystemMaster.PermissionCheckFunction mPermissionCheckOperation;

  /**
   * Create a new instance of {@link InodeSyncStream}.
   *
   * The root path should be already locked with {@link LockPattern#WRITE_EDGE} unless the user is
   * only planning on loading metadata. The desired pattern should always be
   * {@link LockPattern#READ}.
   *
   * It is an error to initiate sync without a WRITE_EDGE lock when loadOnly is {@code false}.
   * If loadOnly is set to {@code true}, then the the root path may have a read lock.
   *
   * @param rootPath The root path to begin syncing
   * @param fsMaster the {@link FileSystemMaster} calling this method
   * @param rpcContext the caller's {@link RpcContext}
   * @param descendantType determines the number of descendant inodes to sync
   * @param options the RPC's {@link FileSystemMasterCommonPOptions}
   * @param auditContext the audit context to use when loading
   * @param auditContextSrcInodeFunc the inode to set as the audit context source
   * @param permissionCheckOperation the operation to use to check permissions
   * @param isGetFileInfo whether the caller is {@link FileSystemMaster#getFileInfo}
   * @param forceSync whether to sync inode metadata no matter what
   * @param loadOnly whether to only load new metadata, rather than update existing metadata
   * @param loadAlways whether to always load new metadata from the ufs, even if a file or
   *                   directory has been previous found to not exist
   */
  public InodeSyncStream(LockingScheme rootPath, DefaultFileSystemMaster fsMaster,
      RpcContext rpcContext, DescendantType descendantType, FileSystemMasterCommonPOptions options,
      @Nullable FileSystemMasterAuditContext auditContext,
      @Nullable Function<LockedInodePath, Inode> auditContextSrcInodeFunc,
      @Nullable DefaultFileSystemMaster.PermissionCheckFunction permissionCheckOperation,
      boolean isGetFileInfo, boolean forceSync, boolean loadOnly, boolean loadAlways) {
    mPendingPaths = new ConcurrentLinkedQueue<>();
    mDescendantType = descendantType;
    mRpcContext = rpcContext;
    mMetadataSyncService = fsMaster.mSyncMetadataExecutor;
    mForceSync = forceSync;
    mRootScheme = rootPath;
    mSyncOptions = options;
    mIsGetFileInfo = isGetFileInfo;
    mLoadOnly = loadOnly;
    mSyncPathJobs = new LinkedList<>();
    mFsMaster = fsMaster;
    mInodeLockManager = fsMaster.getInodeLockManager();
    mInodeStore = fsMaster.getInodeStore();
    mInodeTree = fsMaster.getInodeTree();
    mMountTable = fsMaster.getMountTable();
    mUfsSyncPathCache = fsMaster.getSyncPathCache();
    mAuditContext = auditContext;
    mAuditContextSrcInodeFunc = auditContextSrcInodeFunc;
    mPermissionCheckOperation = permissionCheckOperation;
    // If an absent cache entry was more recent than this value, then it is valid for this sync
    long validCacheTime;
    if (loadOnly) {
      if (loadAlways) {
        validCacheTime = UfsAbsentPathCache.NEVER;
      } else {
        validCacheTime = UfsAbsentPathCache.ALWAYS;
      }
    } else {
      long syncInterval = options.hasSyncIntervalMs() ? options.getSyncIntervalMs() :
          ServerConfiguration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL);
      validCacheTime = System.currentTimeMillis() - syncInterval;
    }
    mStatusCache = new UfsStatusCache(fsMaster.mSyncPrefetchExecutor,
        fsMaster.getAbsentPathCache(), validCacheTime);
  }

  /**
   * Create a new instance of {@link InodeSyncStream} without any audit or permission checks.
   *
   * @param rootScheme The root path to begin syncing
   * @param fsMaster the {@link FileSystemMaster} calling this method
   * @param rpcContext the caller's {@link RpcContext}
   * @param descendantType determines the number of descendant inodes to sync
   * @param options the RPC's {@link FileSystemMasterCommonPOptions}
   * @param isGetFileInfo whether the caller is {@link FileSystemMaster#getFileInfo}
   * @param forceSync whether to sync inode metadata no matter what
   * @param loadOnly whether to only load new metadata, rather than update existing metadata
   * @param loadAlways whether to always load new metadata from the ufs, even if a file or
   *                   directory has been previous found to not exist
   */
  public InodeSyncStream(LockingScheme rootScheme, DefaultFileSystemMaster fsMaster,
      RpcContext rpcContext, DescendantType descendantType, FileSystemMasterCommonPOptions options,
      boolean isGetFileInfo, boolean forceSync, boolean loadOnly, boolean loadAlways) {
    this(rootScheme, fsMaster, rpcContext, descendantType, options, null, null, null,
        isGetFileInfo, forceSync, loadOnly, loadAlways);
  }

  /**
   * Sync the metadata according the the root path the stream was created with.
   *
   * @return SyncStatus object
   */
  public SyncStatus sync() throws AccessControlException, InvalidPathException {
    // The high-level process for the syncing is:
    // 1. Given an Alluxio path, determine if it is not consistent with the corresponding UFS path.
    //     this means the UFS path does not exist, or has metadata which differs from Alluxio
    // 2. If only the metadata changed, update the inode with the new metadata
    // 3. If the path does not exist in the UFS, delete the inode in Alluxio
    // 4. If not deleted, load metadata from the UFS
    // 5. If a recursive sync, add children inodes to sync queue
    int syncPathCount = 0;
    int failedSyncPathCount = 0;
    int stopNum = -1; // stop syncing when we've processed this many paths. -1 for infinite
    if (!mRootScheme.shouldSync() && !mForceSync) {
      return SyncStatus.NOT_NEEDED;
    }
    Instant start = Instant.now();
    try (LockedInodePath path = mInodeTree.lockInodePath(mRootScheme)) {
      if (mAuditContext != null && mAuditContextSrcInodeFunc != null) {
        mAuditContext.setSrcInode(mAuditContextSrcInodeFunc.apply(path));
      }
      try {
        if (mPermissionCheckOperation != null) {
          mPermissionCheckOperation.accept(path, mFsMaster.getPermissionChecker());
        }
      } catch (AccessControlException e) {
        if (mAuditContext != null) {
          mAuditContext.setAllowed(false);
        }
        throw e;
      }
      syncInodeMetadata(path);
      syncPathCount++;
      if (mDescendantType == DescendantType.ONE) {
        // If descendantType is ONE, then we shouldn't process any more paths except for those
        // currently in the queue
        stopNum = mPendingPaths.size();
      }

      // process the sync result for the original path
      try {
        path.traverse();
      } catch (InvalidPathException e) {
        throw new RuntimeException(e);
      }
    } catch (BlockInfoException | FileAlreadyCompletedException
        | FileDoesNotExistException | InterruptedException | InvalidFileSizeException
        | IOException e) {
      LogUtils.warnWithException(LOG, "Failed to sync metadata on root path {}",
          toString(), e);
      failedSyncPathCount++;
    } finally {
      // regardless of the outcome, remove the UfsStatus for this path from the cache
      mStatusCache.remove(mRootScheme.getPath());
    }

    // Process any children after the root.
    while (!mPendingPaths.isEmpty() || !mSyncPathJobs.isEmpty()) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Metadata syncing was interrupted before completion; {}", toString());
        break;
      }
      if (mRpcContext.isCancelled()) {
        LOG.warn("Metadata syncing was cancelled before completion; {}", toString());
        break;
      }
      // There are still paths to process
      // First, remove any futures which have completed. Add to the sync path count if they sync'd
      // successfully
      while (true) {
        Future<Boolean> job = mSyncPathJobs.peek();
        if (job == null || !job.isDone()) {
          break;
        }
        // remove the job because we know it is done.
        if (mSyncPathJobs.poll() != job) {
          throw new ConcurrentModificationException("Head of queue modified while executing");
        }
        try {
          // we synced the path successfully
          // This shouldn't block because we checked job.isDone() earlier
          if (job.get()) {
            syncPathCount++;
          } else {
            failedSyncPathCount++;
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
      if (stopNum != -1 && (syncPathCount + failedSyncPathCount) > stopNum) {
        break;
      }

      // We can submit up to ( max_concurrency - <jobs queue size>) jobs back into the queue
      int submissions = mConcurrencyLevel - mSyncPathJobs.size();
      for (int i = 0; i < submissions; i++) {
        AlluxioURI path = mPendingPaths.poll();
        if (path == null) {
          // no paths left to sync
          break;
        }
        Future<Boolean> job = mMetadataSyncService.submit(() -> processSyncPath(path));
        mSyncPathJobs.offer(job);
      }
      // After submitting all jobs wait for the job at the head of the queue to finish.
      Future<Boolean> oldestJob = mSyncPathJobs.peek();
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
    if (LOG.isDebugEnabled()) {
      Instant end = Instant.now();
      Duration elapsedTime = Duration.between(start, end);
      LOG.debug("synced {} paths ({} success, {} failed) in {} ms on {}",
          syncPathCount + failedSyncPathCount, syncPathCount, failedSyncPathCount,
              elapsedTime.toMillis(), mRootScheme);
    }
    boolean success = syncPathCount > 0;
    if (ServerConfiguration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_REPORT_FAILURE)) {
      // There should not be any failed or outstanding jobs
      success = (failedSyncPathCount == 0) && mSyncPathJobs.isEmpty() && mPendingPaths.isEmpty();
    }
    if (success) {
      // update the sync path cache for the root of the sync
      // TODO(gpang): Do we need special handling for failures and thread interrupts?
      mUfsSyncPathCache.notifySyncedPath(mRootScheme.getPath().getPath(), mDescendantType);
    }
    mStatusCache.cancelAllPrefetch();
    mSyncPathJobs.forEach(f -> f.cancel(true));
    return success ? SyncStatus.OK : SyncStatus.FAILED;
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
  private boolean processSyncPath(AlluxioURI path) {
    if (path == null) {
      return false;
    }
    LockingScheme scheme;
    if (mForceSync) {
      scheme = new LockingScheme(path, LockPattern.READ, true);
    } else {
      scheme = new LockingScheme(path, LockPattern.READ, mSyncOptions,
          mUfsSyncPathCache, mIsGetFileInfo);
    }

    if (!scheme.shouldSync() && !mForceSync) {
      return false;
    }
    try (LockedInodePath inodePath = mInodeTree.tryLockInodePath(scheme)) {
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Thread syncing {} was interrupted before completion", inodePath.getUri());
        return false;
      }
      syncInodeMetadata(inodePath);
      return true;
    } catch (AccessControlException | BlockInfoException | FileAlreadyCompletedException
        | FileDoesNotExistException | InterruptedException | InvalidFileSizeException
        | InvalidPathException | IOException e) {
      LogUtils.warnWithException(LOG, "Failed to process sync path: {}", path, e);
    } finally {
      // regardless of the outcome, remove the UfsStatus for this path from the cache
      mStatusCache.remove(path);
    }
    return false;
  }

  private void syncInodeMetadata(LockedInodePath inodePath)
      throws InvalidPathException, AccessControlException, IOException, FileDoesNotExistException,
      FileAlreadyCompletedException, InvalidFileSizeException, BlockInfoException,
      InterruptedException {
    if (!inodePath.fullPathExists()) {
      loadMetadataForPath(inodePath);
      // skip the load metadata step in the sync if it has been just loaded
      syncExistingInodeMetadata(inodePath, true);
    } else {
      syncExistingInodeMetadata(inodePath, false);
    }
  }

  private Object getFromUfs(Callable<Object> task) throws InterruptedException {
    final Future<Object> future = mFsMaster.mSyncPrefetchExecutor.submit(task);
    while (true) {
      try {
        return future.get(1, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        mRpcContext.throwIfCancelled();
      } catch (ExecutionException e) {
        LogUtils.warnWithException(LOG, "Failed to get result for prefetch job", e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Sync inode metadata with the UFS state.
   *
   * This method expects the {@code inodePath} to already exist in the inode tree.
   */
  private void syncExistingInodeMetadata(LockedInodePath inodePath, boolean skipLoad)
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
    boolean syncChildren = inode.isDirectory() && !mIsGetFileInfo
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
        String ufsFingerprint;
        Fingerprint ufsFpParsed;
        // When the status is not cached and it was not due to file not found, we retry
        if (fileNotFound) {
          ufsFingerprint = Constants.INVALID_UFS_FINGERPRINT;
          ufsFpParsed = Fingerprint.parse(ufsFingerprint);
        } else if (cachedStatus == null) {
          // TODO(david): change the interface so that getFingerprint returns a parsed fingerprint
          ufsFingerprint = (String) getFromUfs(() -> ufs.getFingerprint(ufsUri.toString()));
          ufsFpParsed = Fingerprint.parse(ufsFingerprint);
        } else {
          // When the status is cached
          Pair<AccessControlList, DefaultAccessControlList> aclPair =
              (Pair<AccessControlList, DefaultAccessControlList>)
                  getFromUfs(() -> ufs.getAclPair(ufsUri.toString()));
          if (aclPair == null || aclPair.getFirst() == null || !aclPair.getFirst().hasExtended()) {
            ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus);
          } else {
            ufsFpParsed = Fingerprint.create(ufs.getUnderFSType(), cachedStatus,
                aclPair.getFirst());
          }
          ufsFingerprint = ufsFpParsed.serialize();
        }
        boolean containsMountPoint = mMountTable.containsMountPoint(inodePath.getUri(), true);

        UfsSyncUtils.SyncPlan syncPlan =
            UfsSyncUtils.computeSyncPlan(inode, ufsFpParsed, containsMountPoint);

        if (syncPlan.toUpdateMetaData()) {
          // UpdateMetadata is used when a file or a directory only had metadata change.
          // It works by calling SetAttributeInternal on the inodePath.
          if (ufsFpParsed != null && ufsFpParsed.isValid()) {
            short mode = Short.parseShort(ufsFpParsed.getTag(Fingerprint.Tag.MODE));
            long opTimeMs = System.currentTimeMillis();
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
                .setUfsFingerprint(ufsFingerprint);
            mFsMaster.setAttributeSingleFile(mRpcContext, inodePath, false, opTimeMs, ctx);
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
                .setUnchecked(true));
            mFsMaster.deleteInternal(mRpcContext, inodePath, syncDeleteContext);
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
    }

    Map<String, Inode> inodeChildren = new HashMap<>();
    if (syncChildren) {
      // maps children name to inode
      mInodeStore.getChildren(inode.asDirectory())
          .forEach(child -> inodeChildren.put(child.getName(), child));

      // Fetch and populate children into the cache
      mStatusCache.prefetchChildren(inodePath.getUri(), mMountTable);
      Collection<UfsStatus> listStatus = mStatusCache
          .fetchChildrenIfAbsent(mRpcContext, inodePath.getUri(), mMountTable);
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
      loadMetadataForPath(inodePath);
    }

    if (syncChildren) {
      // Iterate over Alluxio children and process persisted children.
      mInodeStore.getChildren(inode.asDirectory()).forEach(childInode -> {
        // If we are only loading non-existing metadata, then don't process any child which
        // was already in the tree, unless it is a directory, in which case, we might need to load
        // its children.
        if (mLoadOnly && inodeChildren.containsKey(childInode.getName()) && childInode.isFile()) {
          return;
        }
        // If we're performing a recursive sync, add each child of our current Inode to the queue
        AlluxioURI child = inodePath.getUri().joinUnsafe(childInode.getName());
        mPendingPaths.add(child);
        // This asynchronously schedules a job to pre-fetch the statuses into the cache.
        if (childInode.isDirectory() && mDescendantType == DescendantType.ALL) {
          mStatusCache.prefetchChildren(child, mMountTable);
        }
      });
    }
  }

  private void loadMetadataForPath(LockedInodePath inodePath)
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
    LoadMetadataContext ctx = LoadMetadataContext.mergeFrom(
        LoadMetadataPOptions.newBuilder()
            .setCommonOptions(NO_TTL_OPTION)
            .setCreateAncestors(true)
            .setLoadDescendantType(GrpcUtils.toProto(descendantType)))
        .setUfsStatus(status);
    loadMetadata(inodePath, ctx);
  }

  /**
  * This method creates inodes containing the metadata from the UFS. The {@link UfsStatus} object
  * must be set in the {@link LoadMetadataContext} in order to successfully create the inodes.
  */
  private void loadMetadata(LockedInodePath inodePath, LoadMetadataContext context)
      throws AccessControlException, BlockInfoException, FileAlreadyCompletedException,
      FileDoesNotExistException, InvalidFileSizeException, InvalidPathException, IOException {
    AlluxioURI path = inodePath.getUri();
    MountTable.Resolution resolution = mMountTable.resolve(path);
    try {
      if (context.getUfsStatus() == null) {
        // uri does not exist in ufs
        Inode inode = inodePath.getInode();
        if (inode.isFile()) {
          throw new IllegalArgumentException(String.format(
              "load metadata cannot be called on a file if no ufs "
                  + "status is present in the context. %s", inodePath.getUri()));
        }

        mInodeTree.setDirectChildrenLoaded(mRpcContext, inode.asDirectory());
        return;
      }

      if (context.getUfsStatus().isFile()) {
        loadFileMetadataInternal(mRpcContext, inodePath, resolution, context, mFsMaster);
      } else {
        loadDirectoryMetadata(mRpcContext, inodePath, context, mMountTable, mFsMaster);

        // now load all children if required
        LoadDescendantPType type = context.getOptions().getLoadDescendantType();
        if (type != LoadDescendantPType.NONE) {
          Collection<UfsStatus> children = mStatusCache.fetchChildrenIfAbsent(mRpcContext,
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
                    // No Ttl on loaded files
                    .setCommonOptions(NO_TTL_OPTION)
                    .setCreateAncestors(false))
                .setUfsStatus(childStatus);
            try (LockedInodePath descendant = inodePath
                .lockDescendant(inodePath.getUri().joinUnsafe(childStatus.getName()),
                    LockPattern.READ)) {
              loadMetadata(descendant, loadMetadataContext);
            } catch (FileNotFoundException e) {
              LOG.debug("Failed to loadMetadata because file is not in ufs:"
                      + " inodePath={}, options={}.",
                  childURI, loadMetadataContext, e);
            }
          }
          mInodeTree.setDirectChildrenLoaded(mRpcContext, inodePath.getInode().asDirectory());
        }
      }
    } catch (IOException | InterruptedException e) {
      LOG.debug("Failed to loadMetadata: inodePath={}, context={}.", inodePath.getUri(), context,
          e);
      throw new IOException(e);
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
    List<alluxio.proto.journal.Journal.JournalEntry> newEntries = new ArrayList<>();
    // file id : index in the newEntries, InodeFileEntry
    Map<Long, Pair<Integer, MutableInodeFile>> fileEntryMap = new HashMap<>();
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
      // Replace the old entry place holder with the new entry,
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
  static void loadFileMetadataInternal(RpcContext rpcContext, LockedInodePath inodePath,
      MountTable.Resolution resolution, LoadMetadataContext context,
      DefaultFileSystemMaster fsMaster)
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

      if (context.getUfsStatus() == null) {
        context.setUfsStatus(ufs.getExistingFileStatus(ufsUri.toString()));
      }
      ufsLength = ((UfsFileStatus) context.getUfsStatus()).getContentLength();
      long blockSize = ((UfsFileStatus) context.getUfsStatus()).getBlockSize();
      ufsBlockSizeByte = blockSize != UfsFileStatus.UNKNOWN_BLOCK_SIZE
          ? blockSize : ufs.getBlockSizeByte(ufsUri.toString());

      if (fsMaster.isAclEnabled()) {
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
    createFileContext.setWriteType(WriteType.THROUGH); // set as through since already in UFS
    createFileContext.setMetadataLoad(true);
    createFileContext.setOwner(context.getUfsStatus().getOwner());
    createFileContext.setGroup(context.getUfsStatus().getGroup());
    createFileContext.setXAttr(context.getUfsStatus().getXAttr());
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

    try (LockedInodePath writeLockedPath = inodePath.lockFinalEdgeWrite();
         MergeJournalContext merger = new MergeJournalContext(rpcContext.getJournalContext(),
             InodeSyncStream::mergeCreateComplete)) {
      // We do not want to close this wrapRpcContext because it uses elements from another context
      RpcContext wrapRpcContext = new RpcContext(
          rpcContext.getBlockDeletionContext(), merger, rpcContext.getOperationContext());
      fsMaster.createFileInternal(wrapRpcContext, writeLockedPath, createFileContext);
      CompleteFileContext completeContext =
          CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(ufsLength))
              .setUfsStatus(context.getUfsStatus());
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
    if (inodePath.fullPathExists()) {
      return;
    }
    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
    createDirectoryContext.getOptions()
        .setRecursive(context.getOptions().getCreateAncestors()).setAllowExists(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getOptions().getCommonOptions().getTtl())
            .setTtlAction(context.getOptions().getCommonOptions().getTtlAction()));
    createDirectoryContext.setMountPoint(mountTable.isMountPoint(inodePath.getUri()));
    createDirectoryContext.setMetadataLoad(true);
    createDirectoryContext.setWriteType(WriteType.THROUGH);
    MountTable.Resolution resolution = mountTable.resolve(inodePath.getUri());

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
      fsMaster.createDirectoryInternal(rpcContext, writeLockedPath, createDirectoryContext);
    } catch (FileAlreadyExistsException e) {
      // This may occur if a thread created or loaded the directory before we got the write lock.
      // The directory already exists, so nothing needs to be loaded.
    }
    // Re-traverse the path to pick up any newly created inodes.
    inodePath.traverse();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("rootPath", mRootScheme)
        .add("descendantType", mDescendantType)
        .add("commonOptions", mSyncOptions)
        .add("forceSync", mForceSync)
        .add("isGetFileInfo", mIsGetFileInfo)
        .toString();
  }
}
