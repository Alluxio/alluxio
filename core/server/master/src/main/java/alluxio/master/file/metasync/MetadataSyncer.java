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

package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.file.options.DescendantType;
import alluxio.file.options.DirectoryLoadType;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.InternalOperationContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsAbsentPathCache;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.mdsync.BaseTask;
import alluxio.master.mdsync.LoadResult;
import alluxio.master.mdsync.MdSync;
import alluxio.master.mdsync.PathSequence;
import alluxio.master.mdsync.SyncProcess;
import alluxio.master.mdsync.SyncProcessResult;
import alluxio.master.mdsync.TaskGroup;
import alluxio.master.mdsync.TaskTracker;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.SkippableInodeIterator;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.IteratorUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class MetadataSyncer implements SyncProcess {
  public static final FileSystemMasterCommonPOptions NO_TTL_OPTION =
      FileSystemMasterCommonPOptions.newBuilder()
          .setTtl(-1)
          .setTtlAction(
              Configuration.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class))
          .build();
  private static final Logger LOG = LoggerFactory.getLogger(MetadataSyncer.class);
  private final DefaultFileSystemMaster mFsMaster;
  private final ReadOnlyInodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeTree mInodeTree;

  private final TaskTracker mTaskTracker;
  private final MdSync mMdSync;
  private final boolean mIgnoreTTL =
      Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_IGNORE_TTL);
  private final CreateFilePOptions mCreateFilePOptions =
      FileSystemOptionsUtils.createFileDefaults(Configuration.global(), false).toBuilder().build();

  private final Cache<Long, TaskGroup> mTaskGroupMap =
      CacheBuilder.newBuilder().maximumSize(1000).build();
  private final AtomicLong mTaskGroupIds = new AtomicLong(0);

  private final UfsAbsentPathCache mUfsAbsentCache;

  /**
   * Constructs a metadata syncer.
   *
   * @param fsMaster the file system master
   * @param inodeStore the inode store
   * @param mountTable the mount table
   * @param inodeTree the inode tree
   * @param syncPathCache the sync path cache
   * @param absentPathCache the absent path cache
   */
  public MetadataSyncer(
      DefaultFileSystemMaster fsMaster, ReadOnlyInodeStore inodeStore,
      MountTable mountTable, InodeTree inodeTree,
      UfsSyncPathCache syncPathCache, UfsAbsentPathCache absentPathCache) {
    mFsMaster = fsMaster;
    mInodeStore = inodeStore;
    mMountTable = mountTable;
    mInodeTree = inodeTree;
    mTaskTracker = new TaskTracker(
        Configuration.getInt(PropertyKey.MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE),
        Configuration.getInt(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_LOADS),
        Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_GET_STATUS),
        Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_LISTING),
        syncPathCache, absentPathCache, this, this::getUfsClient);
    mMdSync = new MdSync(mTaskTracker);
    mUfsAbsentCache = absentPathCache;
  }

  private static String ufsPathToAlluxioPath(String ufsPath, String ufsMount, String alluxioMount) {
    // first check if the ufsPath is the ufsMount path
    if (ufsPath.length() < ufsMount.length()
        && !ufsPath.endsWith(AlluxioURI.SEPARATOR)) {
      Preconditions.checkState(ufsMount.equals(ufsPath + AlluxioURI.SEPARATOR));
      ufsPath = ufsMount;
    }
    // ufs path will be the full path (but will not include the bucket)
    // e.g. nested/file or /nested/file
    // ufsMount will include the ufs mount path without the bucket, eg /nested/
    // First remove the ufsMount from ufsPath, including the first / so that
    // ufsPath does not start with /
    if (ufsPath.startsWith(AlluxioURI.SEPARATOR)) {
      ufsPath = ufsPath.substring(ufsMount.length());
    } else {
      ufsPath = ufsPath.substring(ufsMount.length() - 1);
    }
    // now append the alluxio mount path to the ufs path
    // the alluxio mount path will be something like /a/b/c
    return alluxioMount + ufsPath;
  }

  /**
   * @param groupId the id of the task group
   * @return the {@link TaskGroup} corresponding to the id
   */
  public Optional<TaskGroup> getTaskGroup(long groupId) {
    return Optional.ofNullable(mTaskGroupMap.getIfPresent(groupId));
  }

  /**
   * Perform a metadata sync on the given path. Launches the task asynchronously.
   * If descendent type is ALL, then a task is launched for each nested mount.
   *
   * @param alluxioPath the path to sync
   * @param descendantType the depth of descendant to load
   * @param directoryLoadType the type of listing to do on directories in the UFS
   * @param syncInterval the sync interval to check if a sync is needed
   * @param isAsyncMetadataLoading if the sync is initiated by an async load metadata cli command
   * @return the running task group
   */
  public TaskGroup syncPath(
      AlluxioURI alluxioPath, DescendantType descendantType, DirectoryLoadType directoryLoadType,
      long syncInterval, boolean isAsyncMetadataLoading) throws InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioPath);
    Stream<BaseTask> tasks = Stream.empty();
    long groupId = mTaskGroupIds.getAndIncrement();
    if (descendantType == DescendantType.ALL) {
      List<MountInfo> nestedMounts = mMountTable.findChildrenMountPoints(alluxioPath, false);
      tasks = nestedMounts.stream().map(mountInfo ->
          mTaskTracker.launchTaskAsync(mMdSync, mountInfo.getUfsUri(), mountInfo.getAlluxioUri(),
              null, descendantType, syncInterval, directoryLoadType, !isAsyncMetadataLoading));
    }
    AlluxioURI ufsPath = resolution.getUri();
    TaskGroup group = new TaskGroup(groupId,
        Stream.concat(Stream.of(mTaskTracker.launchTaskAsync(mMdSync, ufsPath, alluxioPath, null,
        descendantType, syncInterval, directoryLoadType, !isAsyncMetadataLoading)), tasks)
        .toArray(BaseTask[]::new));
    mTaskGroupMap.put(groupId, group);
    return group;
  }

  /**
   * Perform a metadata sync on the given path. Launches the task asynchronously.
   * If descendent type is ALL, then a task is launched for each nested mount, but
   * this method only returns the task launched on the sync path. If tracking for
   * the whole group is needed, then
   * {@link MetadataSyncer#syncPath(AlluxioURI, DescendantType, DirectoryLoadType, long, boolean)}
   * should be used.
   *
   * @param alluxioPath the path to sync
   * @param descendantType the depth of descendents to load
   * @param directoryLoadType the type of listing to do on directories in the UFS
   * @param syncInterval the sync interval to check if a sync is needed
   * @return the running task
   */
  public BaseTask syncPath(
      AlluxioURI alluxioPath, DescendantType descendantType, DirectoryLoadType directoryLoadType,
      long syncInterval) throws InvalidPathException {
    return syncPath(alluxioPath, descendantType, directoryLoadType, syncInterval, false)
        .getBaseTask();
  }

  private CloseableResource<UfsClient> getUfsClient(AlluxioURI ufsPath) {
    CloseableResource<UnderFileSystem> ufsResource =
        getClient(reverseResolve(ufsPath)).acquireUfsResource();
    return new CloseableResource<UfsClient>(ufsResource.get()) {
      @Override
      public void closeResource() {
        ufsResource.closeResource();
      }
    };
  }

  private UfsManager.UfsClient getClient(MountTable.ReverseResolution reverseResolution) {
    UfsManager.UfsClient ufsClient = mMountTable.getUfsClient(
        reverseResolution.getMountInfo().getMountId());
    if (ufsClient == null) {
      throw new NotFoundRuntimeException(String.format("Mount not found for UFS path %s",
          reverseResolution.getMountInfo().getUfsUri()));
    }
    return ufsClient;
  }

  private MountTable.ReverseResolution reverseResolve(
      AlluxioURI ufsPath) throws NotFoundRuntimeException {
    MountTable.ReverseResolution reverseResolution = mMountTable.reverseResolve(
        ufsPath);
    if (reverseResolution == null) {
      throw new NotFoundRuntimeException(String.format("Mount not found for UFS path %s",
          ufsPath));
    }
    return reverseResolution;
  }

  @Override
  public SyncProcessResult performSync(
      LoadResult loadResult, UfsSyncPathCache syncPathCache) throws Throwable {
    try (MetadataSyncContext context =
          MetadataSyncContext.Builder.builder(
              mFsMaster.createNonMergingJournalRpcContext(
                  new InternalOperationContext()), loadResult).build()) {

      MountTable.ReverseResolution reverseResolution
          = reverseResolve(loadResult.getBaseLoadPath());
      try (CloseableResource<UnderFileSystem> ufsResource =
               getClient(reverseResolution).acquireUfsResource()) {
        UnderFileSystem ufs = ufsResource.get();
        final MountInfo mountInfo = reverseResolution.getMountInfo();

        // this is the full mount, eg S3://bucket/dir
        AlluxioURI ufsMountURI = reverseResolution.getMountInfo().getUfsUri();
        // this is the base of the mount, eg s3://bucket/
        AlluxioURI ufsMountBaseUri = new AlluxioURI(ufsMountURI.getRootPath());
        // and without the s3://bucket, e.g. the above would be /dir + /
        final String ufsMountPath = PathUtils.normalizePath(
            ufsMountURI.getPath(), AlluxioURI.SEPARATOR);
        // the loaded and normalized ufs path without the bucket, e.g. /dir/
        final String baseLoadPath = PathUtils.normalizePath(loadResult.getBaseLoadPath().getPath(),
            AlluxioURI.SEPARATOR);

        // the mounted path in alluxio, eg /mount
        AlluxioURI alluxioMountUri = reverseResolution.getMountInfo().getAlluxioUri();
        final String alluxioMountPath = PathUtils.normalizePath(
            alluxioMountUri.getPath(), AlluxioURI.SEPARATOR);
        // the Alluxio path that was loaded from the UFS
        AlluxioURI alluxioSyncPath = reverseResolution.getUri();
        // the completed path sequence is from the previous load's
        // last sync path, until our last UFS item
        AlluxioURI syncStart = new AlluxioURI(ufsPathToAlluxioPath(loadResult.getPreviousLast()
            .orElse(loadResult.getBaseLoadPath()).getPath(), ufsMountPath, alluxioMountPath));
        LOG.debug("Syncing from {}, load batch id {}, load id {}", syncStart,
            loadResult.getLoadRequest().getBatchSetId(),
            loadResult.getLoadRequest().getLoadRequestId());

        Stream<UfsItem> stream = loadResult.getUfsLoadResult().getItems().map(status -> {
          UfsItem item = new UfsItem(status, ufsMountPath, alluxioMountPath);
          try {
            // If we are loading by directory, then we must create a new load task on each
            // directory traversed
            if (loadResult.getTaskInfo().hasDirLoadTasks() && status.isDirectory()
                && !item.mAlluxioUri.isAncestorOf(loadResult.getTaskInfo().getAlluxioPath(), false)
                && !(baseLoadPath.equals(
                PathUtils.normalizePathStart(status.getName(), AlluxioURI.SEPARATOR)))) {
              // first check if the directory needs to be synced
              if (syncPathCache.shouldSyncPath(item.mAlluxioUri,
                  loadResult.getTaskInfo().getSyncInterval(),
                  loadResult.getTaskInfo().getDescendantType()).isShouldSync()) {
                loadResult.getTaskInfo().getMdSync()
                    .loadNestedDirectory(loadResult.getTaskInfo().getId(),
                        ufsMountBaseUri.join(status.getName()));
              }
            }
          } catch (Exception e) {
            throw new InvalidArgumentRuntimeException(e);
          }
          return item;
        });

        PeekingIterator<UfsItem> ufsIterator = new PeekingIterator<>(stream.iterator());
        // Check if the root of the path being synced is a file
        UfsItem firstItem = ufsIterator.peek();
        boolean baseSyncPathIsFile = firstItem != null && firstItem.mUfsItem.isFile()
            && PathUtils.normalizePathStart(firstItem.mUfsItem.getName(), AlluxioURI.SEPARATOR)
            .equals(loadResult.getBaseLoadPath().getPath());

        LOG.debug("Processing sync from {}", firstItem == null ? "" : firstItem.mAlluxioPath);
        // this variable will keep the last UfsStatus returned
        UfsItem lastUfsStatus;
        ReadOption.Builder readOptionBuilder = ReadOption.newBuilder();
        // we start iterating the Alluxio metadata from the end of the
        // previous load batch, or if this is the first load then the base
        // load path
        AlluxioURI readFrom = new AlluxioURI(ufsPathToAlluxioPath(
            loadResult.getPreviousLast().map(AlluxioURI::getPath).orElse(
                baseLoadPath), ufsMountPath, alluxioMountPath));
        // we skip the initial inode if this is not the initial listing, as this
        // inode was processed in the previous listing
        boolean skipInitialReadFrom = loadResult.getPreviousLast().isPresent();
        Preconditions.checkState(readFrom.getPath().startsWith(alluxioMountUri.getPath()));
        loadResult.getPreviousLast().ifPresent(prevLast -> {
          String prevLastAlluxio = ufsPathToAlluxioPath(
              prevLast.getPath(), ufsMountPath, alluxioMountPath);
          String readFromSubstring = prevLastAlluxio.substring(
              alluxioSyncPath.getPath().endsWith(AlluxioURI.SEPARATOR)
                  ? alluxioSyncPath.getPath().length() : alluxioSyncPath.getPath().length() + 1);
          readOptionBuilder.setReadFrom(readFromSubstring);
        });
        // We stop iterating the Alluxio metadata at the last loaded item if the load result
        // is truncated
        AlluxioURI readUntil = null;
        if (loadResult.getUfsLoadResult().isTruncated()
            && loadResult.getUfsLoadResult().getLastItem().isPresent()) {
          readUntil = new AlluxioURI(ufsPathToAlluxioPath(
              loadResult.getUfsLoadResult().getLastItem().get().getPath(),
              ufsMountPath, alluxioMountPath));
        }

        // Take the root of the sync path as a write_edge, once we traverse
        // past this node, we will downgrade it to a read lock in
        // SyncProcessState.getNextInode
        LockingScheme lockingScheme = new LockingScheme(alluxioSyncPath,
            InodeTree.LockPattern.WRITE_EDGE, false);
        try (LockedInodePath lockedInodePath =
                 mInodeTree.lockInodePath(
                     lockingScheme, context.getRpcContext().getJournalContext())) {
          // after taking the lock on the root path,
          // we must verify the mount is still valid
          String ufsMountUriString = PathUtils.normalizePath(ufsMountPath, "/");
          String ufsMountUriStringAfterTakingLock =
              PathUtils.normalizePath(
                  mMountTable.resolve(alluxioSyncPath).getUfsMountPointUri().getPath(), "/");
          if (!ufsMountUriString.equals(ufsMountUriStringAfterTakingLock)) {
            NotFoundRuntimeException ex = new NotFoundRuntimeException(String.format(
                "Mount path %s no longer exists during sync of %s",
                ufsMountURI, alluxioSyncPath));
            handleConcurrentModification(context, alluxioSyncPath.getPath(), true, ex);
            throw ex;
          }
          // Get the inode of the sync start
          try (SkippableInodeIterator inodeIterator = mInodeStore.getSkippableChildrenIterator(
              readOptionBuilder.build(), context.getDescendantType(), loadResult.isFirstLoad(),
              lockedInodePath)) {
            SyncProcessState syncState = new SyncProcessState(alluxioMountPath,
                alluxioSyncPath, lockedInodePath, loadResult.isFirstLoad(),
                readFrom, skipInitialReadFrom, readUntil,
                context, inodeIterator, ufsIterator, mountInfo, ufs);
            lastUfsStatus = updateMetadataSync(syncState);
          }
          if (lockedInodePath.fullPathExists() && lockedInodePath.getInode().isDirectory()
              && !lockedInodePath.getInode().asDirectory().isDirectChildrenLoaded()) {
            // check if the root sync path should have its children marked as loaded
            context.addDirectoriesToUpdateIsChildrenLoaded(lockedInodePath.getUri());
          }
        }
        context.updateDirectChildrenLoaded(mInodeTree);
        context.updateAbsentCache(mUfsAbsentCache);
        AlluxioURI syncEnd = lastUfsStatus == null ? syncStart
            : lastUfsStatus.mAlluxioUri;
        PathSequence pathSequence = new PathSequence(syncStart, syncEnd);
        LOG.debug("Completed processing sync from {} until {}", syncStart, syncEnd);
        return new SyncProcessResult(loadResult.getTaskInfo(), loadResult.getBaseLoadPath(),
            pathSequence, loadResult.getUfsLoadResult().isTruncated(),
            baseSyncPathIsFile);
      }
    }
  }

  private UfsItem updateMetadataSync(SyncProcessState syncState)
      throws IOException, FileDoesNotExistException, FileAlreadyExistsException, BlockInfoException,
      AccessControlException, DirectoryNotEmptyException, InvalidPathException {
    InodeIterationResult currentInode = syncState.getNextInode();
    if (currentInode != null && currentInode.getLockedPath().getUri().equals(
        syncState.mMountInfo.getAlluxioUri())) {
      // skip the inode of the mount path
      currentInode = syncState.getNextInode();
    }
    // We don't want to include the inode that we are reading from, so skip until we are sure
    // we are passed that
    while (syncState.mUfsStatusIterator.hasNext() && currentInode != null
        && ((syncState.mSkipInitialReadFrom
        && syncState.mReadFrom.compareTo(currentInode.getLockedPath().getUri()) >= 0)
        || (!syncState.mSkipInitialReadFrom
        && syncState.mReadFrom.compareTo(currentInode.getLockedPath().getUri()) > 0))) {
      currentInode = syncState.getNextInode();
    }
    UfsItem currentUfsStatus = IteratorUtils.nextOrNull(
        syncState.mUfsStatusIterator);
    // skip the initial mount path of the UFS status
    // as well as the base sync path if this is not our first load task
    if (currentUfsStatus != null
        && (currentUfsStatus.mAlluxioPath.equals(syncState.mAlluxioMountPath)
        || (!syncState.mIsFirstLoad
        && currentUfsStatus.mAlluxioUri.equals(syncState.mAlluxioSyncPath)))) {
      currentUfsStatus = IteratorUtils.nextOrNull(
          syncState.mUfsStatusIterator);
    }
    UfsItem lastUfsStatus = currentUfsStatus;

    // Case A. Alluxio /foo and UFS /bar
    //    1. WRITE_LOCK lock /bar
    //    2. create /bar
    //    3. unlock /bar
    //    4. move UFS pointer
    // Case B. Alluxio /bar and UFS /foo
    //    1. WRITE_LOCK lock /bar
    //    2. delete /bar RECURSIVELY (call fs master)
    //    3. unlock /bar
    //    4. move Alluxio pointer and SKIP the children of /foo
    // Case C. Alluxio /foo and Alluxio /foo
    //    1. compare the fingerprint
    //    2. WRITE_LOCK /foo
    //    3. update the metadata
    //    4. unlock /foo
    //    5. move two pointers
    while (currentInode != null || currentUfsStatus != null) {
      SingleInodeSyncResult result = performSyncOne(syncState, currentUfsStatus, currentInode);
      if (result.mSkipChildren) {
        syncState.mInodeIterator.skipChildrenOfTheCurrent();
      }
      if (result.mMoveInode) {
        currentInode = syncState.getNextInode();
      }
      if (result.mMoveUfs) {
        currentUfsStatus = IteratorUtils.nextOrNull(syncState.mUfsStatusIterator);
        lastUfsStatus = currentUfsStatus == null ? lastUfsStatus : currentUfsStatus;
      }
    }
    Preconditions.checkState(!syncState.mUfsStatusIterator.hasNext());
    return lastUfsStatus;
  }

  private void checkShouldSetDescendantsLoaded(Inode inode, SyncProcessState syncState)
      throws FileDoesNotExistException, InvalidPathException {
    // Mark directories as having their children loaded based on the sync descendent type
    if (syncState.mContext.getDescendantType() != DescendantType.NONE) {
      if (inode.isDirectory() && !inode.asDirectory().isDirectChildrenLoaded()) {
        AlluxioURI inodePath = mInodeTree.getPath(inode.getId());
        // The children have been loaded if
        // (1) The descendant type is ALL and the inode is contained in the sync path
        // (2) The descendant type is ONE and the inode is the synced path
        if ((syncState.mContext.getDescendantType() == DescendantType.ALL
            && syncState.mAlluxioSyncPath.isAncestorOf(inodePath, false))
            || (syncState.mContext.getDescendantType() == DescendantType.ONE
            && syncState.mAlluxioSyncPath.equals(inodePath))) {
          syncState.mContext.addDirectoriesToUpdateIsChildrenLoaded(inodePath);
        }
      }
    }
  }

  protected SingleInodeSyncResult performSyncOne(
      SyncProcessState syncState,
      @Nullable UfsItem currentUfsStatus,
      @Nullable InodeIterationResult currentInode)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException, BlockInfoException, DirectoryNotEmptyException, AccessControlException {

    Optional<Integer> comparisonResult = currentInode != null && currentUfsStatus != null
        ? Optional.of(
        currentInode.getLockedPath().getUri().compareTo(currentUfsStatus.mAlluxioUri)) :
        Optional.empty();
    if (currentInode == null || (comparisonResult.isPresent() && comparisonResult.get() > 0)) {
      // (Case 1) - in this case the UFS item is missing in the inode tree, so we create it
      // comparisonResult is present implies that currentUfsStatus is not null
      assert currentUfsStatus != null;
      try (LockedInodePath lockedInodePath = syncState.mAlluxioSyncPathLocked.lockDescendant(
          currentUfsStatus.mAlluxioUri, InodeTree.LockPattern.WRITE_EDGE)) {
        List<Inode> createdInodes;
        if (currentUfsStatus.mUfsItem.isDirectory()) {
          createdInodes = createInodeDirectoryMetadata(syncState.mContext, lockedInodePath,
              currentUfsStatus.mUfsItem, syncState);
        } else {
          createdInodes = createInodeFileMetadata(syncState.mContext, lockedInodePath,
              currentUfsStatus.mUfsItem, syncState);
        }
        if (syncState.mContext.getDescendantType() != DescendantType.NONE) {
          // Mark directories as having their children loaded based on the sync descendent type
          for (Inode next : createdInodes) {
            checkShouldSetDescendantsLoaded(next, syncState);
          }
        }
        syncState.mContext.reportSyncOperationSuccess(SyncOperation.CREATE, createdInodes.size());
      } catch (FileAlreadyExistsException e) {
        handleConcurrentModification(
            syncState.mContext, currentUfsStatus.mAlluxioPath, false, e);
      }
      return new SingleInodeSyncResult(true, false, false);
    } else if (currentUfsStatus == null || comparisonResult.get() < 0) {
      if (currentInode.getInode().isDirectory() && currentUfsStatus != null
          && currentInode.getLockedPath().getUri().isAncestorOf(
              currentUfsStatus.mAlluxioUri, false)) {
        // (Case 2) - in this case the inode is a directory and is an ancestor of the current
        // UFS state, so we skip it
        checkShouldSetDescendantsLoaded(currentInode.getInode(), syncState);
        return new SingleInodeSyncResult(false, true, false);
      }
      // (Case 3) - in this case the inode is not in the UFS, so we must delete it
      try {
        LockedInodePath path = currentInode.getLockedPath();
        path.traverse();
        // skip if this is a mount point
        if (mMountTable.isMountPoint(currentInode.getLockedPath().getUri())) {
          return new SingleInodeSyncResult(false, true, true);
        }
        syncState.mContext.reportSyncOperationSuccess(SyncOperation.DELETE,
            deletePath(syncState.mContext, path));
      } catch (FileDoesNotExistException e) {
        handleConcurrentModification(
            syncState.mContext, currentInode.getLockedPath().getUri().getPath(), false, e);
      }
      return new SingleInodeSyncResult(false, true, true);
    }
    // (Case 4) - in this case both the inode, and the UFS item exist, so we check if we need
    // to update the metadata
    LockedInodePath lockedInodePath = currentInode.getLockedPath();
    lockedInodePath.traverse();
    // skip if this is a mount point
    if (mMountTable.isMountPoint(currentInode.getLockedPath().getUri())) {
      return new SingleInodeSyncResult(true, true, true);
    }
    // HDFS also fetches ACL list, which is ignored for now
    String ufsType = syncState.mUfs.getUnderFSType();
    Fingerprint ufsFingerprint = Fingerprint.create(ufsType, currentUfsStatus.mUfsItem);
    boolean containsMountPoint = mMountTable.containsMountPoint(
        currentInode.getLockedPath().getUri(), true, false);
    UfsSyncUtils.SyncPlan syncPlan =
        UfsSyncUtils.computeSyncPlan(currentInode.getInode(), ufsFingerprint, containsMountPoint);
    if (syncPlan.toUpdateMetaData() || syncPlan.toDelete() || syncPlan.toLoadMetadata()) {
      try {
        if (syncPlan.toUpdateMetaData()) {
          /*
            Inode obtained from the iterator might be stale, especially if the inode is persisted
            in the rocksDB. This in generally should not cause issues. But if the sync plan is to
            update the inode, the inode reference might not be the inode we want to update, and we
            might do wrong updates in an incorrect inode.
            e.g. considering the following steps:
              1. create a file /file in alluxio, the file is persisted in UFS
              2. change the MODE of /file, without applying this change in UFS
              3. do a metadata sync on /file, as the fingerprints are different,
                 /file is expected to be updated using the metadata from UFS
              4. during the metadata sync, the file /file is deleted from alluxio, a DIRECTORY
                 with the same name /file is created
              5. the metadata of /file in UFS will be used to update the directory /file, which
                 is incorrect.
              To prevent this from happening, we re-fetch the inode from the locked inode path
              and makes sure the reference is the same, before we update the inode.
           */
          Inode inode = lockedInodePath.getInode();
          if (inode.getId() != currentInode.getInode().getId()
              || !currentInode.getInode().getUfsFingerprint().equals(inode.getUfsFingerprint())) {
            handleConcurrentModification(
                syncState.mContext, lockedInodePath.getUri().getPath(),
                false,
                new FileAlreadyExistsException(
                    "Inode has been concurrently modified during metadata sync."));
          } else {
            updateInodeMetadata(syncState.mContext, lockedInodePath, currentUfsStatus.mUfsItem,
                ufsFingerprint);
            syncState.mContext.reportSyncOperationSuccess(SyncOperation.UPDATE);
          }
        } else if (syncPlan.toDelete() && syncPlan.toLoadMetadata()) {
          if (lockedInodePath.getInode().isDirectory()) {
            LOG.warn("Deleting directory {} in metadata sync due to metadata change",
                lockedInodePath.getUri());
          }
          deletePath(syncState.mContext, lockedInodePath);
          lockedInodePath.removeLastInode();
          try (LockedInodePath newLockedInodePath = mInodeTree.lockInodePath(
              lockedInodePath.getUri(), InodeTree.LockPattern.WRITE_EDGE,
              syncState.mContext.getMetadataSyncJournalContext())) {
            if (currentUfsStatus.mUfsItem.isDirectory()) {
              createInodeDirectoryMetadata(syncState.mContext, newLockedInodePath,
                  currentUfsStatus.mUfsItem, syncState);
            } else {
              createInodeFileMetadata(syncState.mContext, newLockedInodePath,
                  currentUfsStatus.mUfsItem, syncState);
            }
          }
          syncState.mContext.reportSyncOperationSuccess(SyncOperation.RECREATE);
        } else {
          throw new IllegalStateException("We should never reach here.");
        }
      } catch (FileDoesNotExistException | FileAlreadyExistsException e) {
        handleConcurrentModification(
            syncState.mContext, currentInode.getLockedPath().getUri().getPath(), false, e);
      }
    } else {
      syncState.mContext.reportSyncOperationSuccess(SyncOperation.NOOP);
    }
    checkShouldSetDescendantsLoaded(currentInode.getInode(), syncState);
    return new SingleInodeSyncResult(true, true, false);
  }

  private void handleConcurrentModification(
      MetadataSyncContext context, String path, boolean isRoot, Exception e)
      throws FileAlreadyExistsException, FileDoesNotExistException {
    String loggingMessage = "Sync metadata failed on [{}] due to concurrent modification.";
    if (!isRoot && context.isConcurrentModificationAllowed()) {
      context.reportSyncOperationSuccess(SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION);
      LOG.info(loggingMessage, path, e);
    } else {
      context.reportSyncFailReason(SyncFailReason.PROCESSING_CONCURRENT_UPDATE_DURING_SYNC, e);
      LOG.error(loggingMessage, path, e);
      if (e instanceof FileAlreadyExistsException) {
        throw (FileAlreadyExistsException) e;
      }
      if (e instanceof FileDoesNotExistException) {
        throw (FileDoesNotExistException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private int deletePath(MetadataSyncContext context, LockedInodePath lockedInodePath)
      throws FileDoesNotExistException, DirectoryNotEmptyException, IOException,
      InvalidPathException {
    DeleteContext syncDeleteContext = DeleteContext.mergeFrom(
            DeletePOptions.newBuilder()
                .setRecursive(true)
                .setAlluxioOnly(true)
                .setUnchecked(true))
        .setMetadataLoad(true);
    return mFsMaster.deleteInternal(context.getRpcContext(),
        lockedInodePath, syncDeleteContext, true);
  }

  private void updateInodeMetadata(
      MetadataSyncContext context, LockedInodePath lockedInodePath,
      UfsStatus ufsStatus, Fingerprint fingerprint)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException {
    // UpdateMetadata is used when a file or a directory only had metadata change.
    // It works by calling SetAttributeInternal on the inodePath.
    short mode = ufsStatus.getMode();
    SetAttributePOptions.Builder builder = SetAttributePOptions.newBuilder()
        .setMode(new Mode(mode).toProto());
    if (!ufsStatus.getOwner().equals("")) {
      builder.setOwner(ufsStatus.getOwner());
    }
    if (!ufsStatus.getGroup().equals("")) {
      builder.setOwner(ufsStatus.getGroup());
    }
    SetAttributeContext ctx = SetAttributeContext.mergeFrom(builder)
        .setUfsFingerprint(fingerprint.serialize())
        .setMetadataLoad(true);
    // Why previously clock is used?
    mFsMaster.setAttributeSingleFile(context.getRpcContext(), lockedInodePath, false,
        CommonUtils.getCurrentMs(), ctx);
  }

  private List<Inode> createInodeFileMetadata(
      MetadataSyncContext context, LockedInodePath lockedInodePath,
      UfsStatus ufsStatus, SyncProcessState syncState
  ) throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      BlockInfoException, IOException {
    long blockSize = ((UfsFileStatus) ufsStatus).getBlockSize();
    if (blockSize == UfsFileStatus.UNKNOWN_BLOCK_SIZE) {
      throw new RuntimeException("Unknown block size");
    }

    // Metadata loaded from UFS has no TTL set.
    CreateFileContext createFileContext = CreateFileContext.mergeFromDefault(mCreateFilePOptions);
    createFileContext.getOptions().setBlockSizeBytes(blockSize);
    // Ancestor should be created before unless it is the sync root
    createFileContext.getOptions().setRecursive(true);
    FileSystemMasterCommonPOptions commonPOptions =
        mIgnoreTTL ? NO_TTL_OPTION : context.getCommonOptions();
    createFileContext.getOptions()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(commonPOptions.getTtl())
            .setTtlAction(commonPOptions.getTtlAction()));
    Fingerprint fingerprint = Fingerprint.create(syncState.mUfs.getUnderFSType(), ufsStatus);
    createFileContext.setMissingDirFingerprint(() ->
        Fingerprint.create(syncState.mUfs.getUnderFSType(),
            new UfsDirectoryStatus(
                ufsStatus.getName(), ufsStatus.getOwner(),
                ufsStatus.getGroup(), ufsStatus.getMode())).serialize());
    createFileContext.setFingerprint(fingerprint.serialize());

    createFileContext.setWriteType(WriteType.THROUGH); // set as through since already in UFS
    createFileContext.setMetadataLoad(true, false);
    createFileContext.setOwner(ufsStatus.getOwner());
    createFileContext.setGroup(ufsStatus.getGroup());
    createFileContext.setXAttr(ufsStatus.getXAttr());
    short ufsMode = ufsStatus.getMode();
    Mode mode = new Mode(ufsMode);
    Long ufsLastModified = ufsStatus.getLastModifiedTime();
    if (syncState.mMountInfo.getOptions().getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileContext.getOptions().setMode(mode.toProto());
    // NO ACL for now
    if (ufsLastModified != null) {
      createFileContext.setOperationTimeMs(ufsLastModified);
    }
    List<Inode> result = mFsMaster.createCompleteFileInternalForMetadataSync(
        context.getRpcContext(), lockedInodePath, createFileContext, (UfsFileStatus) ufsStatus);
    context.addDirectoriesToUpdateAbsentCache(lockedInodePath.getUri().getParent());
    return result;
  }

  private List<Inode> createInodeDirectoryMetadata(
      MetadataSyncContext context, LockedInodePath lockedInodePath,
      UfsStatus ufsStatus, SyncProcessState syncState
  ) throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException {
    MountTable.Resolution resolution = mMountTable.resolve(lockedInodePath.getUri());
    boolean isMountPoint = mMountTable.isMountPoint(lockedInodePath.getUri());

    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext.defaults();
    createDirectoryContext.getOptions()
        .setRecursive(true)
        .setAllowExists(false)
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(context.getCommonOptions().getTtl())
            .setTtlAction(context.getCommonOptions().getTtlAction()));
    createDirectoryContext.setMountPoint(isMountPoint);
    createDirectoryContext.setMetadataLoad(true, false);
    createDirectoryContext.setWriteType(WriteType.THROUGH);
    String dirFingerprint = Fingerprint.create(
        syncState.mUfs.getUnderFSType(), ufsStatus).serialize();
    createDirectoryContext.setMissingDirFingerprint(() -> dirFingerprint);
    createDirectoryContext.setFingerprint(dirFingerprint);

    String ufsOwner = ufsStatus.getOwner();
    String ufsGroup = ufsStatus.getGroup();
    short ufsMode = ufsStatus.getMode();
    Long lastModifiedTime = ufsStatus.getLastModifiedTime();
    Mode mode = new Mode(ufsMode);
    if (resolution.getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createDirectoryContext.getOptions().setMode(mode.toProto());
    createDirectoryContext
        .setOwner(ufsOwner)
        .setGroup(ufsGroup)
        .setUfsStatus(ufsStatus);
    createDirectoryContext.setXAttr(ufsStatus.getXAttr());

    if (lastModifiedTime != null) {
      createDirectoryContext.setOperationTimeMs(lastModifiedTime);
    }
    return mFsMaster.createDirectoryInternal(
        context.getRpcContext(),
        lockedInodePath,
        resolution.getUfsClient(),
        resolution.getUri(),
        createDirectoryContext
    );
  }

  /**
   * @return the task tracker
   */
  public TaskTracker getTaskTracker() {
    return mTaskTracker;
  }

  static final class UfsItem {
    final UfsStatus mUfsItem;
    final String mAlluxioPath;
    final AlluxioURI mAlluxioUri;

    UfsItem(UfsStatus ufsStatus, String ufsMount, String alluxioMount) {
      mAlluxioPath = ufsPathToAlluxioPath(ufsStatus.getName(), ufsMount, alluxioMount);
      mAlluxioUri = new AlluxioURI(mAlluxioPath);
      mUfsItem = ufsStatus;
    }
  }

  @VisibleForTesting
  static final class SyncProcessState {
    final String mAlluxioMountPath;
    final AlluxioURI mAlluxioSyncPath;
    final LockedInodePath mAlluxioSyncPathLocked;
    final AlluxioURI mReadFrom;
    final boolean mSkipInitialReadFrom;
    final AlluxioURI mReadUntil;
    final MetadataSyncContext mContext;
    final SkippableInodeIterator mInodeIterator;
    final Iterator<UfsItem> mUfsStatusIterator;
    final MountInfo mMountInfo;
    final UnderFileSystem mUfs;
    final boolean mIsFirstLoad;
    boolean mTraversedRootPath = false;
    boolean mDowngradedRootPath = false;

    SyncProcessState(
        String alluxioMountPath,
        AlluxioURI alluxioSyncPath,
        LockedInodePath alluxioSyncPathLocked,
        boolean isFirstLoad,
        AlluxioURI readFrom, boolean skipInitialReadFrom,
        @Nullable AlluxioURI readUntil,
        MetadataSyncContext context,
        SkippableInodeIterator inodeIterator,
        Iterator<UfsItem> ufsStatusIterator,
        MountInfo mountInfo, UnderFileSystem underFileSystem) {
      mAlluxioMountPath = alluxioMountPath;
      mAlluxioSyncPath = alluxioSyncPath;
      mAlluxioSyncPathLocked = alluxioSyncPathLocked;
      mIsFirstLoad = isFirstLoad;
      mReadFrom = readFrom;
      mSkipInitialReadFrom = skipInitialReadFrom;
      mReadUntil = readUntil;
      mContext = context;
      mInodeIterator = inodeIterator;
      mUfsStatusIterator = ufsStatusIterator;
      mMountInfo = mountInfo;
      mUfs = underFileSystem;
    }

    private void downgradeRootPath() {
      // once we have traversed the root sync path we downgrade it to a read lock
      mAlluxioSyncPathLocked.downgradeToRead();
      mDowngradedRootPath = true;
    }

    @Nullable
    InodeIterationResult getNextInode() throws InvalidPathException {
      if (mTraversedRootPath && !mDowngradedRootPath) {
        downgradeRootPath();
      }
      mTraversedRootPath = true;
      InodeIterationResult next = IteratorUtils.nextOrNull(mInodeIterator);
      if (next != null) {
        if (!mAlluxioSyncPath.isAncestorOf(next.getLockedPath().getUri(), false)) {
          downgradeRootPath();
          return null;
        }
        if (mReadUntil != null) {
          if (next.getLockedPath().getUri().compareTo(mReadUntil) > 0) {
            downgradeRootPath();
            return null;
          }
        }
      }
      return next;
    }
  }

  protected static class SingleInodeSyncResult {
    boolean mMoveUfs;
    boolean mMoveInode;
    boolean mSkipChildren;

    public SingleInodeSyncResult(boolean moveUfs, boolean moveInode, boolean skipChildren) {
      mMoveUfs = moveUfs;
      mMoveInode = moveInode;
      mSkipChildren = skipChildren;
    }
  }
}
