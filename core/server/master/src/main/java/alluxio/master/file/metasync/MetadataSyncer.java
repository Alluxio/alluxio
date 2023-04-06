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
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.exception.runtime.NotFoundRuntimeException;
import alluxio.exception.status.UnavailableException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.InodeSyncStream;
import alluxio.master.file.MetadataSyncLockManager;
import alluxio.master.file.RpcContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeIterationResult;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.LockingScheme;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.mdsync.BaseTask;
import alluxio.master.mdsync.DirectoryLoadType;
import alluxio.master.mdsync.LoadResult;
import alluxio.master.mdsync.MdSync;
import alluxio.master.mdsync.PathSequence;
import alluxio.master.mdsync.SyncProcess;
import alluxio.master.mdsync.SyncProcessResult;
import alluxio.master.mdsync.TaskTracker;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.SkippableInodeIterator;
import alluxio.resource.CloseableResource;
import alluxio.security.authorization.Mode;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsClient;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.s3a.AlluxioS3Exception;
import alluxio.util.CommonUtils;
import alluxio.util.IteratorUtils;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.iterators.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class MetadataSyncer implements SyncProcess {
  public static final FileSystemMasterCommonPOptions NO_TTL_OPTION =
      FileSystemMasterCommonPOptions.newBuilder()
          .setTtl(-1)
          .build();
  private static final Logger LOG = LoggerFactory.getLogger(MetadataSyncer.class);
  /**
   * To determine whether we should only let the UFS sync happen once
   * for the concurrent metadata sync requests syncing the same directory.
   */
  private final boolean mDedupConcurrentSync = Configuration.getBoolean(
      PropertyKey.MASTER_METADATA_CONCURRENT_SYNC_DEDUP
  );
  private final DefaultFileSystemMaster mFsMaster;
  private final ReadOnlyInodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeTree mInodeTree;
  private final TaskTracker mTaskTracker;
  private final MdSync mMdSync;
  /**
   * A {@link UfsSyncPathCache} maintained from the {@link DefaultFileSystemMaster}.
   */
  private final UfsSyncPathCache mUfsSyncPathCache;
  private final boolean mIgnoreTTL =
      Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_IGNORE_TTL);

  /**
   * Constructs a metadata syncer.
   *
   * @param fsMaster      the file system master
   * @param inodeStore    the inode store
   * @param mountTable    the mount table
   * @param inodeTree     the inode tree
   * @param syncPathCache the sync path cache
   */
  public MetadataSyncer(
      DefaultFileSystemMaster fsMaster, ReadOnlyInodeStore inodeStore,
      MountTable mountTable, InodeTree inodeTree, UfsSyncPathCache syncPathCache) {
    mFsMaster = fsMaster;
    mInodeStore = inodeStore;
    mMountTable = mountTable;
    mInodeTree = inodeTree;
    mUfsSyncPathCache = syncPathCache;
    mTaskTracker = new TaskTracker(
        Configuration.getInt(PropertyKey.MASTER_METADATA_SYNC_EXECUTOR_POOL_SIZE),
        Configuration.getInt(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_LOADS),
        Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_GET_STATUS),
        Configuration.getBoolean(PropertyKey.MASTER_METADATA_SYNC_UFS_CONCURRENT_LISTING),
        syncPathCache, this, this::getUfsClient);
    mMdSync = new MdSync(mTaskTracker);
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
    UfsManager.UfsClient ufsClient =  mMountTable.getUfsClient(
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

  private static String ufsPathToAlluxioPath(String ufsPath, String ufsMount, String alluxioMount) {
    // ufs path will be the full path (but will not include the bucket)
    // e.g. nested/afile or /nested/afile
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

  public BaseTask syncPath(
      AlluxioURI alluxioPath, DescendantType descendantType,
      long syncInterval, long timeoutMs) throws Throwable {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioPath);
    AlluxioURI ufsPath = resolution.getUri();
    Pair<Boolean, BaseTask> result = mTaskTracker.checkTask(mMdSync, ufsPath, null,
        descendantType, syncInterval, DirectoryLoadType.NONE);
    result.getSecond().waitComplete(timeoutMs);
    return result.getSecond();
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

  static final class SyncProcessState {
    final String mAlluxioMountPath;
    final AlluxioURI mAlluxioSyncPath;
    final AlluxioURI mReadFrom;
    final boolean mSkipInitialReadFrom;
    final String mUfsMountPath;
    final AlluxioURI mReadUntil;
    final MetadataSyncContext mContext;
    final SkippableInodeIterator mInodeIterator;
    final Iterator<UfsItem> mUfsStatusIterator;
    final MountInfo mMountInfo;
    final UnderFileSystem mUfs;

    SyncProcessState(
        String alluxioMountPath,
        AlluxioURI alluxioSyncPath,
        AlluxioURI readFrom, boolean skipInitialReadFrom,
        String ufsMountPath,
        @Nullable AlluxioURI readUntil,
        MetadataSyncContext context,
        SkippableInodeIterator inodeIterator,
        Iterator<UfsItem> ufsStatusIterator,
        MountInfo mountInfo, UnderFileSystem underFileSystem) {
      mAlluxioMountPath = alluxioMountPath;
      mAlluxioSyncPath = alluxioSyncPath;
      mReadFrom = readFrom;
      mUfsMountPath = ufsMountPath;
      mSkipInitialReadFrom = skipInitialReadFrom;
      mReadUntil = readUntil;
      mContext = context;
      mInodeIterator = inodeIterator;
      mUfsStatusIterator = ufsStatusIterator;
      mMountInfo = mountInfo;
      mUfs = underFileSystem;
    }

    @Nullable InodeIterationResult getNextInode() throws InvalidPathException {
      InodeIterationResult next = IteratorUtils.nextOrNull(mInodeIterator);
      if (next != null) {
        if (!mAlluxioSyncPath.isAncestorOf(next.getLockedPath().getUri())) {
          return null;
        }
        if (mReadUntil != null) {
          if (next.getLockedPath().getUri().compareTo(mReadUntil) > 0) {
            return null;
          }
        }
      }
      return next;
    }
  }

  @Override
  public SyncProcessResult performSync(
      LoadResult loadResult, UfsSyncPathCache syncPathCache) throws Throwable {

    try (RpcContext rpcContext = mFsMaster.createRpcContext()) {
      MetadataSyncContext context =
          MetadataSyncContext.Builder.builder(rpcContext, loadResult.getTaskInfo()
              .getDescendantType()).build();
      context.startSync();

      MountTable.ReverseResolution reverseResolution
          = reverseResolve(loadResult.getTaskInfo().getBasePath());
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

        // the mounted path in alluxio, eg /mount
        AlluxioURI alluxioMountUri = reverseResolution.getMountInfo().getAlluxioUri();
        final String alluxioMountPath = PathUtils.normalizePath(
            alluxioMountUri.getPath(), AlluxioURI.SEPARATOR);

        Stream<UfsItem> stream = loadResult.getUfsLoadResult().getItems().map(status -> {
          // If we are loading by directory, then we must create a new load task on each
          // directory traversed
          if (loadResult.getTaskInfo().hasDirLoadTasks() && status.isDirectory()) {
            try {
              AlluxioURI fullPath = new AlluxioURI(ufsPathToAlluxioPath(
                  status.getName(), ufsMountPath, alluxioMountPath));
              // first check if the directory needs to be synced
              if (syncPathCache.shouldSyncPath(
                  reverseResolve(fullPath).getUri(),
                  loadResult.getTaskInfo().getSyncInterval(),
                  loadResult.getTaskInfo().getDescendantType()).isShouldSync()) {
                loadResult.getTaskInfo().getMdSync()
                    .loadNestedDirectory(loadResult.getTaskInfo().getId(), fullPath);
              }
            } catch (InvalidPathException e) {
              throw new InvalidArgumentRuntimeException(e);
            }
          }
          return new UfsItem(status, ufsMountPath, alluxioMountPath);
        });

        PeekingIterator<UfsItem> ufsIterator = new PeekingIterator<>(stream.iterator());
        // Check if the root of the path being synced is a file
        UfsItem firstItem = ufsIterator.peek();
        boolean baseSyncPathIsFile = firstItem != null && firstItem.mUfsItem.isFile()
            && PathUtils.normalizePathStart(firstItem.mUfsItem.getName(), AlluxioURI.SEPARATOR)
            .equals(loadResult.getBaseLoadPath().getPath());

        // this variable will keep the last UfsStatus returned
        UfsItem lastUfsStatus;
        ReadOption.Builder readOptionBuilder = ReadOption.newBuilder();
        // we start iterating the Alluxio metadata from the end of the
        // previous load batch, or if this is the first load than the base
        // load path
        AlluxioURI readFrom = new AlluxioURI(ufsPathToAlluxioPath(
            loadResult.getPreviousLast().map(AlluxioURI::getPath).orElse(
                PathUtils.normalizePath(loadResult.getBaseLoadPath().getPath(),
                    AlluxioURI.SEPARATOR)), ufsMountPath, alluxioMountPath));
        // we skip the initial inode if this is not the initial listing, as this
        // inode was processed in the previous listing
        boolean skipInitialReadFrom = loadResult.getPreviousLast().isPresent();
        Preconditions.checkState(readFrom.getPath().startsWith(alluxioMountUri.getPath()));
        String readFromSubstring = readFrom.getPath().substring(
            alluxioMountUri.getPath().length());
        if (!readFromSubstring.isEmpty()) {
          readOptionBuilder.setReadFrom(readFromSubstring);
        }
        // We stop iterating the Alluxio metadata at the last loaded item if the load result
        // is truncated
        AlluxioURI readUntil = null;
        if (loadResult.getUfsLoadResult().isTruncated()
            && loadResult.getUfsLoadResult().getLastItem().isPresent()) {
          readUntil = new AlluxioURI(ufsPathToAlluxioPath(
              loadResult.getUfsLoadResult().getLastItem().get().getPath(),
              ufsMountPath, alluxioMountPath));
        }

        AlluxioURI alluxioSyncPath = reverseResolution.getUri();
        LockingScheme lockingScheme = new LockingScheme(alluxioSyncPath,
            InodeTree.LockPattern.WRITE_EDGE, false);
        try (LockedInodePath lockedInodePath =
                 mInodeTree.lockInodePath(lockingScheme, rpcContext.getJournalContext())) {

          // Get the inode of the mount root
          try (SkippableInodeIterator inodeIterator = mInodeStore.getSkippableChildrenIterator(
              readOptionBuilder.build(), context.isRecursive(), lockedInodePath)) {

            SyncProcessState syncState = new SyncProcessState(alluxioMountPath,
                alluxioSyncPath,
                readFrom, skipInitialReadFrom, ufsMountPath, readUntil,
                context, inodeIterator, ufsIterator, mountInfo, ufs);
            lastUfsStatus = updateMetadataSync(syncState);
          } catch (IOException | AlluxioS3Exception e) {
            handleUfsIOException(context, readFrom, e);
            // TODO(tcrain)
            // return context.fail();
            throw e;
          } catch (AlluxioException e) {
            // TODO(tcrain)
            // return context.fail();
            throw e;
          }
        }
        // TODO(tcrain)
        // return context.success();
        // the completed path sequence is from the previous last sync path, until our last UFS item
        AlluxioURI syncStart = loadResult.getPreviousLast().orElse(
            loadResult.getBaseLoadPath());
        AlluxioURI syncEnd = lastUfsStatus == null ? syncStart
            : ufsMountBaseUri.join(lastUfsStatus.mUfsItem.getName());
        return new SyncProcessResult(loadResult.getTaskInfo(), loadResult.getBaseLoadPath(),
            new PathSequence(syncStart, syncEnd), loadResult.getUfsLoadResult().isTruncated(),
            baseSyncPathIsFile, context.success(), loadResult.isFirstLoad());
      }
    }
  }

  private UfsItem updateMetadataSync(SyncProcessState syncState)
      throws IOException, FileDoesNotExistException, FileAlreadyExistsException, BlockInfoException,
      AccessControlException, DirectoryNotEmptyException, InvalidPathException {
    // We don't want to include the inode that we are reading from, so skip until we are sure
    // we are passed that
    InodeIterationResult currentInode = syncState.getNextInode();
    if (currentInode != null && currentInode.getLockedPath().getUri().equals(
        syncState.mMountInfo.getAlluxioUri())) {
      // skip the inode of the mount path
      currentInode = syncState.getNextInode();
    }
    while (syncState.mUfsStatusIterator.hasNext() && currentInode != null
        && ((syncState.mSkipInitialReadFrom
        && syncState.mReadFrom.compareTo(currentInode.getLockedPath().getUri()) >= 0)
        || (!syncState.mSkipInitialReadFrom
        && syncState.mReadFrom.compareTo(currentInode.getLockedPath().getUri()) > 0))) {
      currentInode = syncState.getNextInode();
    }
    UfsItem currentUfsStatus = IteratorUtils.nextOrNullUnwrapIOException(
        syncState.mUfsStatusIterator);
    if (currentUfsStatus != null
        && currentUfsStatus.mAlluxioPath.equals(syncState.mAlluxioMountPath)) {
      // skip the initial mount path
      currentUfsStatus = IteratorUtils.nextOrNullUnwrapIOException(
          syncState.mUfsStatusIterator);
    }
    UfsItem lastUfsStatus = currentUfsStatus;

    if (syncState.mContext.isRecursive() && currentUfsStatus != null
        && currentUfsStatus.mUfsItem.isDirectory()) {
      syncState.mContext.addDirectoriesToUpdateIsChildrenLoaded(currentUfsStatus.mAlluxioUri);
    }

    // Case a. Alluxio /foo and UFS /bar
    //    1. WRITE_LOCK lock /bar
    //    2. create /bar
    //    3. unlock /bar
    //    4. move UFS pointer
    // Case b. Alluxio /bar and UFS /foo
    //    1. WRITE_LOCK lock /bar
    //    2. delete /bar RECURSIVELY (call fs master)
    //    3. unlock /bar
    //    4. move Alluxio pointer and SKIP the children of /foo
    // Case c. Alluxio /foo and Alluxio /foo
    //    1. compare the fingerprint
    //    2. WRITE_LOCK /foo
    //    3. update the metadata
    //    4. unlock /foo
    //    5. move two pointers
    while (currentInode != null || currentUfsStatus != null) {
      if (currentInode == null) {
        System.out.println("Inode null");
      } else {
        System.out.println("Inode " + currentInode.getName());
      }
      if (currentUfsStatus == null) {
        System.out.println("Ufs null");
      } else {
        System.out.println("Ufs " + currentUfsStatus.mAlluxioPath);
      }

      SingleInodeSyncResult result = performSyncOne(syncState, currentUfsStatus, currentInode);
      if (result.mLoadChildrenMountPoint) {
        // TODO(tcrain) this should be submitted as a job when the initial task is created
        // sync(syncRootPath.join(currentInode.getName()), context);
      }
      if (result.mSkipChildren) {
        syncState.mInodeIterator.skipChildrenOfTheCurrent();
      }
      if (result.mMoveInode) {
        currentInode = syncState.getNextInode();
      }
      if (result.mMoveUfs) {
        currentUfsStatus = IteratorUtils.nextOrNullUnwrapIOException(syncState.mUfsStatusIterator);
        lastUfsStatus = currentUfsStatus == null ? lastUfsStatus : currentUfsStatus;
        if (syncState.mContext.isRecursive() && currentUfsStatus != null
            && currentUfsStatus.mUfsItem.isDirectory()) {
          syncState.mContext.addDirectoriesToUpdateIsChildrenLoaded(
              currentUfsStatus.mAlluxioUri);
        }
      }
    }
    Preconditions.checkState(!syncState.mUfsStatusIterator.hasNext());
    return lastUfsStatus;
  }

  protected SingleInodeSyncResult performSyncOne(
      SyncProcessState syncState,
      @Nullable UfsItem currentUfsStatus,
      @Nullable InodeIterationResult currentInode)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException, BlockInfoException, DirectoryNotEmptyException, AccessControlException {
    // Check if the inode is the mount point, if yes, just skip this one
    // Skip this check for the sync root where the name of the current inode is ""
    if (currentInode != null && !currentInode.getName().equals("")) {
      boolean isMountPoint = mMountTable.isMountPoint(currentInode.getLockedPath().getUri());
      if (isMountPoint) {
        // Skip the ufs if it is shadowed by the mount point
        boolean skipUfs = currentUfsStatus != null
            && currentUfsStatus.mAlluxioPath.equals(
                currentInode.getLockedPath().getUri().getPath());
        if (skipUfs) {
          syncState.mContext.reportSyncOperationSuccess(SyncOperation.SKIPPED_ON_MOUNT_POINT);
        }
        return new SingleInodeSyncResult(
            skipUfs, true, true, syncState.mContext.getDescendantType() == DescendantType.ALL);
      }
    }

    Optional<Integer> comparisonResult = currentInode != null && currentUfsStatus != null
        ? Optional.of(
            currentInode.getLockedPath().getUri().compareTo(currentUfsStatus.mAlluxioUri)) :
        Optional.empty();
    if (currentInode == null || (comparisonResult.isPresent() && comparisonResult.get() > 0)) {
      // (Case 1) - in this case the UFS item is missing in the inode tree, so we create it
      // comparisonResult is present implies that currentUfsStatus is not null
      assert currentUfsStatus != null;
      try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
          currentUfsStatus.mAlluxioUri, InodeTree.LockPattern.WRITE_EDGE,
          NoopJournalContext.INSTANCE)) {
        if (currentUfsStatus.mUfsItem.isDirectory()) {
          createInodeDirectoryMetadata(syncState.mContext, lockedInodePath,
              currentUfsStatus.mUfsItem);
        } else {
          createInodeFileMetadata(syncState.mContext, lockedInodePath, currentUfsStatus.mUfsItem,
              syncState.mMountInfo);
        }
        syncState.mContext.reportSyncOperationSuccess(SyncOperation.CREATE);
      } catch (FileAlreadyExistsException e) {
        handleConcurrentModification(
            syncState.mContext, currentUfsStatus.mAlluxioPath, false, e);
      }
      return new SingleInodeSyncResult(true, false, false, false);
    } else if (currentUfsStatus == null || comparisonResult.get() < 0) {
      if (currentInode.getInode().isDirectory() && currentUfsStatus != null
          && currentInode.getLockedPath().getUri().isAncestorOf(currentUfsStatus.mAlluxioUri)) {
        // (Case 2) - in this case the inode is a directory and is an ancestor of the current
        // UFS state, so we skip it
        return new SingleInodeSyncResult(false, true, false, false);
      }
      // (Case 3) - in this case the inode is not in the UFS, so we must delete it
      try {
        deleteFile(syncState.mContext, currentInode.getLockedPath());
        syncState.mContext.reportSyncOperationSuccess(SyncOperation.DELETE);
      } catch (FileDoesNotExistException e) {
        handleConcurrentModification(
            syncState.mContext, currentInode.getLockedPath().getUri().getPath(), false, e);
      }
      return new SingleInodeSyncResult(false, true, true, false);
    }
    // (Case 4) - in this case both the inode, and the UFS item exist, so we check if we need
    // to update the metadata
    // HDFS also fetches ACL list, which is ignored for now
    String ufsType = syncState.mUfs.getUnderFSType();
    Fingerprint ufsFingerprint = Fingerprint.create(ufsType, currentUfsStatus.mUfsItem);
    boolean containsMountPoint = mMountTable.containsMountPoint(
        currentInode.getLockedPath().getUri(), true);
    UfsSyncUtils.SyncPlan syncPlan =
        UfsSyncUtils.computeSyncPlan(currentInode.getInode(), ufsFingerprint, containsMountPoint);
    if (syncPlan.toUpdateMetaData() || syncPlan.toDelete() || syncPlan.toLoadMetadata()) {
      try {
        LockedInodePath lockedInodePath = currentInode.getLockedPath();
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
          deleteFile(syncState.mContext, lockedInodePath);
          lockedInodePath.removeLastInode();
          if (currentUfsStatus.mUfsItem.isDirectory()) {
            createInodeDirectoryMetadata(syncState.mContext, lockedInodePath,
                currentUfsStatus.mUfsItem);
          } else {
            createInodeFileMetadata(syncState.mContext, lockedInodePath,
                currentUfsStatus.mUfsItem, syncState.mMountInfo);
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
    return new SingleInodeSyncResult(true, true, false, false);
  }















  // TODO list:
  // directory Fingerprint -> WIP
  // sync file vs. directory
  // sync empty directory
  // update sync time? -> done
  // update is direct children loaded -> WIP
  // metrics -> WIP
  // performance?
  // concurrent sync dedup?
  // race condition (lock failed) -> addressing
  // path prefix related (e.g. the startAfter param)
  // error handling -> WIP
  // continuation sync

  /**
   * Performs a metadata sync.
   *
   * @param path    the path to sync
   * @param context the metadata sync context
   * @return the metadata sync result
   */
  public SyncResult sync(AlluxioURI path, MetadataSyncContext context)
      throws AccessControlException, InvalidPathException, UnavailableException {
    if (!mDedupConcurrentSync) {
      return syncInternal(path, context);
    }
    try (MetadataSyncLockManager.MetadataSyncPathList ignored =
             InodeSyncStream.SYNC_METADATA_LOCK_MANAGER.lockPath(
                 path)) {
      context.getRpcContext().throwIfCancelled();
      // TODO check if the sync should still be performed and skip as needed if
      // a concurrent sync happens.
      return syncInternal(path, context);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SyncResult syncInternal(AlluxioURI path, MetadataSyncContext context)
      throws AccessControlException, InvalidPathException, UnavailableException {
    System.out.println("Syncing...");
    context.validateStartAfter(path);
    context.startSync();
    long startTime = CommonUtils.getCurrentMs();
    // TODO what if this path doesn't map to a ufs path
    // TODO when start after is set, check if the sync root does not contain any nested mount points
    try (CloseableResource<UnderFileSystem> ufs = mMountTable.resolve(path).acquireUfsResource()) {
      MountTable.Resolution resolution = mMountTable.resolve(path);

      UfsStatus ufsSyncPathRoot = null;
      Inode inodeSyncRoot = null;
      try {
        ufsSyncPathRoot = ufs.get().getStatus(resolution.getUri().getPath());
        ufsSyncPathRoot.setName("");
      } catch (FileNotFoundException ignored) {
        // No-op
      } catch (IOException | AlluxioS3Exception e) {
        handleUfsIOException(context, path, e);
        return context.fail();
      }
      // TODO how to handle race condition here
      try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
          path, InodeTree.LockPattern.READ, context.getRpcContext().getJournalContext())) {
        inodeSyncRoot = lockedInodePath.getInodeOrNull();
      }
      InodeIterationResult inodeWithName = null;
      if (inodeSyncRoot != null) {
        inodeWithName = new InodeIterationResult(inodeSyncRoot, "", null, null);
      }
      if (inodeSyncRoot == null && ufsSyncPathRoot == null) {
        // TODO or should we throw an exception?
        context.setFailReason(SyncFailReason.FILE_DOES_NOT_EXIST);
        return context.fail();
      }
      System.out.println("-------Syncing root-------------");
      System.out.println("Sync root path: " + path);
      if (ufsSyncPathRoot != null) {
        context.ufsFileScanned();
      }
      try {
        syncOne(context, path, ufsSyncPathRoot, inodeWithName, true);
      } catch (AlluxioException | IOException e) {
        return context.fail();
      }
      if (context.getDescendantType() == DescendantType.NONE
          || ufsSyncPathRoot != null && ufsSyncPathRoot.isFile()) {
        return context.success();
      }

      System.out.println("-------Syncing children-------------");
      final Iterator<UfsStatus> ufsStatusIterator;
      try {
        ufsStatusIterator = ufs.get().listStatusIterable(
            resolution.getUri().toString(),
            ListOptions.defaults().setRecursive(context.isRecursive()),
            context.getStartAfter(),
            // TODO check the batch size.
            // if the batch size is < than # of '/', the batch fetch will result in
            // endless loop.
            context.getBatchSize()
        );
      } catch (IOException | AlluxioS3Exception e) {
        handleUfsIOException(context, path, e);
        return context.fail();
      }
      // Directory does not exist
      if (ufsStatusIterator == null) {
        return context.success();
      }

      long syncRootInodeId = mFsMaster.getFileId(path);
      ReadOption.Builder readOptionBuilder = ReadOption.newBuilder();
      readOptionBuilder.setReadFrom(context.getStartAfter());
      try (SkippableInodeIterator inodeIterator = mInodeStore.getSkippableChildrenIterator(
          readOptionBuilder.build(), context.isRecursive(), null)) {
        updateMetadata(path, context, inodeIterator, ufsStatusIterator, context.getStartAfter());
      } catch (IOException | AlluxioS3Exception e) {
        handleUfsIOException(context, path, e);
        return context.fail();
      } catch (AlluxioException e) {
        return context.fail();
      }
      try (LockedInodePath lockedInodePath = mInodeTree.lockFullInodePath(
          path, InodeTree.LockPattern.WRITE_INODE, context.getRpcContext().getJournalContext())) {
        if (lockedInodePath.getInode().isDirectory()) {
          mInodeTree.setDirectChildrenLoaded(() -> context.getRpcContext().getJournalContext(),
              lockedInodePath.getInode().asDirectory());
        }
        mUfsSyncPathCache.notifySyncedPath(
            path, context.getDescendantType(), startTime, null, lockedInodePath.getInode().isFile()
        );
      } catch (FileDoesNotExistException e) {
        LOG.error("Sync root {} is modified during the metadata sync.", path, e);
        context.setFailReason(SyncFailReason.CONCURRENT_UPDATE_DURING_SYNC);
        context.fail();
      }
      context.updateDirectChildrenLoaded(mInodeTree);
      return context.success();
    }
  }

  /**
   * Performs a metadata sync asynchronously and return a job id (?).
   */
  public void syncAsync() {
  }

  // Path loader
  private void loadPaths() {
  }

  // UFS loader
  private void loadMetadataFromUFS() {
  }

  private UfsStatus updateMetadata(
      AlluxioURI syncRootPath,
      MetadataSyncContext context,
      SkippableInodeIterator alluxioInodeIterator,
      Iterator<UfsStatus> ufsStatusIterator,
      @Nullable String startFrom
  ) throws IOException, FileDoesNotExistException, FileAlreadyExistsException, BlockInfoException,
      AccessControlException, DirectoryNotEmptyException, InvalidPathException {
    InodeIterationResult currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
    UfsStatus currentUfsStatus = IteratorUtils.nextOrNullUnwrapIOException(ufsStatusIterator);
    UfsStatus lastUfsStatus = currentUfsStatus;

    // If startFrom is not null, then this means the metadata sync was previously failed,
    // and resumed by the user. Listing with a startAfter may include parent directories
    // of the first listed object.
    // e.g. if startFrom = /a/b/c
    // /a and /a/b might also be included in the iterators.
    // We skip the processing of these as they are supposed to be taken care by
    // the previous sync already.
    if (startFrom != null) {
      while (currentInode != null && currentInode.getName().compareTo(startFrom) <= 0) {
        currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
      }

      while (currentUfsStatus != null && currentUfsStatus.getName().compareTo(startFrom) <= 0) {
        currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
        lastUfsStatus = currentUfsStatus == null ? lastUfsStatus : currentUfsStatus;
      }
    }

    if (context.isRecursive() && currentUfsStatus != null && currentUfsStatus.isDirectory()) {
      context.addDirectoriesToUpdateIsChildrenLoaded(
          syncRootPath.join(currentUfsStatus.getName()));
    }

    // Case a. Alluxio /foo and UFS /bar
    //    1. WRITE_LOCK lock /bar
    //    2. create /bar
    //    3. unlock /bar
    //    4. move UFS pointer
    // Case b. Alluxio /bar and UFS /foo
    //    1. WRITE_LOCK lock /bar
    //    2. delete /bar RECURSIVELY (call fs master)
    //    3. unlock /bar
    //    4. move Alluxio pointer and SKIP the children of /foo
    // Case c. Alluxio /foo and Alluxio /foo
    //    1. compare the fingerprint
    //    2. WRITE_LOCK /foo
    //    3. update the metadata
    //    4. unlock /foo
    //    5. move two pointers
    while (currentInode != null || currentUfsStatus != null) {
      /*
      if (currentInode == null) {
        System.out.println("Inode null");
      } else {
        System.out.println("Inode " + currentInode.getName());
      }
      if (currentUfsStatus == null) {
        System.out.println("Ufs null");
      } else {
        System.out.println("Ufs " + currentUfsStatus.getName());
      }
       */

      SingleInodeSyncResult result = syncOne(
          context, syncRootPath, currentUfsStatus, currentInode, false);
      if (result.mLoadChildrenMountPoint) {
        // TODO this should be submitted as a job
        sync(syncRootPath.join(currentInode.getName()), context);
      }
      if (result.mSkipChildren) {
        alluxioInodeIterator.skipChildrenOfTheCurrent();
      }
      if (result.mMoveInode) {
        currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
      }
      if (result.mMoveUfs) {
        currentUfsStatus = IteratorUtils.nextOrNullUnwrapIOException(ufsStatusIterator);
        lastUfsStatus = currentUfsStatus == null ? lastUfsStatus : currentUfsStatus;
        if (context.isRecursive() && currentUfsStatus != null && currentUfsStatus.isDirectory()) {
          context.addDirectoriesToUpdateIsChildrenLoaded(
              syncRootPath.join(currentUfsStatus.getName()));
        }
        if (currentUfsStatus != null) {
          context.ufsFileScanned();
        }
      }
    }
    Preconditions.checkState(!ufsStatusIterator.hasNext());
    return lastUfsStatus;
  }

  protected SingleInodeSyncResult syncOne(
      MetadataSyncContext context,
      AlluxioURI syncRootPath,
      @Nullable UfsStatus currentUfsStatus,
      @Nullable InodeIterationResult currentInode,
      boolean isSyncRoot)
      throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      IOException, BlockInfoException, DirectoryNotEmptyException, AccessControlException {
    // Check if the inode is the mount point, if yes, just skip this one
    // Skip this check for the sync root where the name of the current inode is ""
    if (currentInode != null && !currentInode.getName().equals("")) {
      boolean isMountPoint = mMountTable.isMountPoint(syncRootPath.join(currentInode.getName()));
      if (isMountPoint) {
        // Skip the ufs if it is shadowed by the mount point
        boolean skipUfs = currentUfsStatus != null
            && currentUfsStatus.getName().equals(currentInode.getName());
        if (skipUfs) {
          context.reportSyncOperationSuccess(SyncOperation.SKIPPED_ON_MOUNT_POINT);
        }
        return new SingleInodeSyncResult(
            skipUfs, true, true, context.getDescendantType() == DescendantType.ALL);
      }
    }

    Optional<Integer> comparisonResult = currentInode != null && currentUfsStatus != null
        ? Optional.of(currentInode.getName().compareTo(currentUfsStatus.getName())) :
        Optional.empty();
    if (currentInode == null || (comparisonResult.isPresent() && comparisonResult.get() > 0)) {
      // (Case 1) - in this case the UFS item is missing in the inode tree, so we create it
      // comparisonResult is present implies that currentUfsStatus is not null
      assert currentUfsStatus != null;
      try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
          syncRootPath.join(currentUfsStatus.getName()), InodeTree.LockPattern.WRITE_EDGE,
          NoopJournalContext.INSTANCE)) {
        if (currentUfsStatus.isDirectory()) {
          createInodeDirectoryMetadata(context, lockedInodePath, currentUfsStatus);
        } else {
          createInodeFileMetadata(context, lockedInodePath, currentUfsStatus, null);
        }
        context.reportSyncOperationSuccess(SyncOperation.CREATE);
      } catch (FileAlreadyExistsException e) {
        handleConcurrentModification(
            context, syncRootPath.join(currentUfsStatus.getName()).getPath(), isSyncRoot, e);
      }
      return new SingleInodeSyncResult(true, false, false, false);
    } else if (currentUfsStatus == null || comparisonResult.get() < 0) {
      // (Case 2) - in this case the inode is not in the UFS, so we must delete it
      try (LockedInodePath lockedInodePath = mInodeTree.lockFullInodePath(
          syncRootPath.join(currentInode.getName()), InodeTree.LockPattern.WRITE_EDGE,
          NoopJournalContext.INSTANCE)) {
        deleteFile(context, lockedInodePath);
        context.reportSyncOperationSuccess(SyncOperation.DELETE);
      } catch (FileDoesNotExistException e) {
        handleConcurrentModification(
            context, syncRootPath.join(currentInode.getName()).getPath(), isSyncRoot, e);
      }
      return new SingleInodeSyncResult(false, true, true, false);
    }
    // (Case 3) - in this case both the inode, and the UFS item exist, so we check if we need
    // to update the metadata
    // HDFS also fetches ACL list, which is ignored for now
    MountTable.Resolution resolution =
        mMountTable.resolve(syncRootPath.join(currentInode.getName()));
    final String ufsType;
    try (CloseableResource<UnderFileSystem> ufs = resolution.acquireUfsResource()) {
      ufsType = ufs.get().getUnderFSType();
    }
    Fingerprint ufsFingerprint = Fingerprint.create(ufsType, currentUfsStatus);
    boolean containsMountPoint = mMountTable.containsMountPoint(
        syncRootPath.join(currentInode.getName()), true);
    UfsSyncUtils.SyncPlan syncPlan =
        UfsSyncUtils.computeSyncPlan(currentInode.getInode(), ufsFingerprint, containsMountPoint);
    if (syncPlan.toUpdateMetaData() || syncPlan.toDelete() || syncPlan.toLoadMetadata()) {
      try (LockedInodePath lockedInodePath = mInodeTree.lockFullInodePath(
          syncRootPath.join(currentInode.getName()), InodeTree.LockPattern.WRITE_EDGE,
          NoopJournalContext.INSTANCE)) {
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
                context, syncRootPath.join(inode.getName()).getPath(),
                isSyncRoot,
                new FileAlreadyExistsException(
                    "Inode has been concurrently modified during metadata sync."));
          } else {
            updateInodeMetadata(context, lockedInodePath, currentUfsStatus, ufsFingerprint);
            context.reportSyncOperationSuccess(SyncOperation.UPDATE);
          }
        } else if (syncPlan.toDelete() && syncPlan.toLoadMetadata()) {
          deleteFile(context, lockedInodePath);
          lockedInodePath.removeLastInode();
          if (currentUfsStatus.isDirectory()) {
            createInodeDirectoryMetadata(context, lockedInodePath, currentUfsStatus);
          } else {
            createInodeFileMetadata(context, lockedInodePath, currentUfsStatus, null);
          }
          context.reportSyncOperationSuccess(SyncOperation.RECREATE);
        } else {
          throw new IllegalStateException("We should never reach here.");
        }
      } catch (FileDoesNotExistException | FileAlreadyExistsException e) {
        handleConcurrentModification(
            context, syncRootPath.join(currentInode.getName()).getPath(), isSyncRoot, e);
      }
    } else {
      context.reportSyncOperationSuccess(SyncOperation.NOOP);
    }
    return new SingleInodeSyncResult(true, true, false, false);
  }

  private void handleUfsIOException(MetadataSyncContext context, AlluxioURI path, Exception e) {
    LOG.error("Sync on {} failed due to UFS IO Exception", path.toString(), e);
    context.setFailReason(SyncFailReason.UFS_IO_FAILURE);
  }

  private void handleConcurrentModification(
      MetadataSyncContext context, String path, boolean isRoot, AlluxioException e)
      throws FileAlreadyExistsException, FileDoesNotExistException {
    String loggingMessage = String.format(
        "Sync metadata failed on [%s] due to concurrent modification.", path);
    if (!isRoot && context.isConcurrentModificationAllowed()) {
      context.reportSyncOperationSuccess(SyncOperation.SKIPPED_DUE_TO_CONCURRENT_MODIFICATION);
      LOG.info(loggingMessage, e);
    } else {
      context.setFailReason(SyncFailReason.CONCURRENT_UPDATE_DURING_SYNC);
      LOG.error(loggingMessage, e);
      if (e instanceof FileAlreadyExistsException) {
        throw (FileAlreadyExistsException) e;
      }
      if (e instanceof FileDoesNotExistException) {
        throw (FileDoesNotExistException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private void deleteFile(MetadataSyncContext context, LockedInodePath lockedInodePath)
      throws FileDoesNotExistException, DirectoryNotEmptyException, IOException,
      InvalidPathException {
    DeleteContext syncDeleteContext = DeleteContext.mergeFrom(
            DeletePOptions.newBuilder()
                .setRecursive(true)
                .setAlluxioOnly(true)
                .setUnchecked(true))
        .setMetadataLoad(true);
    mFsMaster.deleteInternal(context.getRpcContext(), lockedInodePath, syncDeleteContext, true);
    //System.out.println("Deleted file " + lockedInodePath.getUri());
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
    //System.out.println("Updated file " + lockedInodePath.getUri());
  }

  private void createInodeFileMetadata(
      MetadataSyncContext context, LockedInodePath lockedInodePath,
      UfsStatus ufsStatus, MountInfo mountInfo
  ) throws InvalidPathException, FileDoesNotExistException, FileAlreadyExistsException,
      BlockInfoException, IOException {
    // TODO add metrics

    long blockSize = ((UfsFileStatus) ufsStatus).getBlockSize();
    if (blockSize == UfsFileStatus.UNKNOWN_BLOCK_SIZE) {
      // TODO if UFS never returns the block size, this might fail
      // then we should consider falling back to ufs.getBlockSizeByte()
      throw new RuntimeException("Unknown block size");
    }

    // Metadata loaded from UFS has no TTL set.
    CreateFileContext createFileContext = CreateFileContext.defaults();
    createFileContext.getOptions().setBlockSizeBytes(blockSize);
    // Ancestor should be created before unless it is the sync root
    createFileContext.getOptions().setRecursive(true);
    FileSystemMasterCommonPOptions commonPOptions =
        mIgnoreTTL ? NO_TTL_OPTION : context.getCommonOptions();
    createFileContext.getOptions()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(commonPOptions.getTtl())
            .setTtlAction(commonPOptions.getTtlAction()));
    createFileContext.setWriteType(WriteType.THROUGH); // set as through since already in UFS
    createFileContext.setMetadataLoad(true);
    createFileContext.setOwner(ufsStatus.getOwner());
    createFileContext.setGroup(ufsStatus.getGroup());
    createFileContext.setXAttr(ufsStatus.getXAttr());
    short ufsMode = ufsStatus.getMode();
    Mode mode = new Mode(ufsMode);
    Long ufsLastModified = ufsStatus.getLastModifiedTime();
    // TODO see if this can be optimized
    if (mountInfo.getOptions().getShared()) {
      mode.setOtherBits(mode.getOtherBits().or(mode.getOwnerBits()));
    }
    createFileContext.getOptions().setMode(mode.toProto());
    // NO ACL for now
    if (ufsLastModified != null) {
      createFileContext.setOperationTimeMs(ufsLastModified);
    }
    mFsMaster.createCompleteFileInternalForMetadataSync(
        context.getRpcContext(), lockedInodePath, createFileContext, (UfsFileStatus) ufsStatus);
    //System.out.println("Created file " + lockedInodePath.getUri());
  }

  private void createInodeDirectoryMetadata(
      MetadataSyncContext context, LockedInodePath lockedInodePath,
      UfsStatus ufsStatus
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
    createDirectoryContext.setMetadataLoad(true);
    createDirectoryContext.setWriteType(WriteType.THROUGH);

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
    mFsMaster.createDirectoryInternal(
        context.getRpcContext(),
        lockedInodePath,
        resolution.getUfsClient(),
        resolution.getUri(),
        createDirectoryContext
    );
    //System.out.println("Created directory " + lockedInodePath.getUri());
  }

  protected static class SingleInodeSyncResult {
    boolean mMoveUfs;
    boolean mMoveInode;
    boolean mSkipChildren;
    boolean mLoadChildrenMountPoint;

    public SingleInodeSyncResult(boolean moveUfs, boolean moveInode, boolean skipChildren,
                                 boolean loadChildrenMountPoint) {
      mMoveUfs = moveUfs;
      mMoveInode = moveInode;
      mSkipChildren = skipChildren;
      mLoadChildrenMountPoint = loadChildrenMountPoint;
    }
  }
}
