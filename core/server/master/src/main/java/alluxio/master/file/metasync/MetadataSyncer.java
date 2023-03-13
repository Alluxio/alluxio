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
import alluxio.exception.InvalidPathException;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.file.meta.UfsSyncUtils;
import alluxio.master.journal.NoopJournalContext;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.master.metastore.ReadOption;
import alluxio.master.metastore.RecursiveInodeIterator;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListPartialOptions;
import alluxio.util.IteratorUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class MetadataSyncer {
  /** The FS master creating this object. */
  private final DefaultFileSystemMaster mFsMaster;
  private final ReadOnlyInodeStore mInodeStore;
  private final MountTable mMountTable;
  private final InodeTree mInodeTree;

  public MetadataSyncer(
      DefaultFileSystemMaster fsMaster, ReadOnlyInodeStore inodeStore,
      MountTable mountTable, InodeTree inodeTree) {
    mFsMaster = fsMaster;
    mInodeStore = inodeStore;
    mMountTable = mountTable;
    mInodeTree = inodeTree;
  }


  /**
   * Performs a metadata sync.
   * @param path the path to sync
   * @param isRecursive if children should be loaded recursively
   * @return the metadata sync result
   */
  public SyncResult sync(AlluxioURI path, boolean isRecursive)
      throws Exception {
    System.out.println("Syncing...");
    UnderFileSystem ufs = mMountTable.resolve(path).acquireUfsResource().get();
    // do we need ->
    //mMountTable.resolve(path).getUfsMountPointUri();
    ListPartialOptions opts = ListPartialOptions.defaults().setRecursive(isRecursive);
    opts.mBatchSize = 2;
    UfsStatus previousItem = null;
    while (true) {
      System.out.println("------ Iterating ------");
      UnderFileSystem.PartialListingResult result = ufs.listStatusPartial(path.getPath(), opts);
      updateMetadata(path, previousItem, result.getUfsStatuses(), isRecursive, opts.mStartAfter);
      previousItem = result.getUfsStatuses()[result.getUfsStatuses().length-1];
      opts.mStartAfter = previousItem.getName();
      if (!result.shouldFetchNext() || result.getUfsStatuses().length == 0) {
        updateMetadata(path, previousItem, new UfsStatus[0], isRecursive, opts.mStartAfter);
        break;
      }
    }

    return new SyncResult(false, 0);
  }

  /**
   * Performs a metadata sync asynchronously and return a job id (?).
   */
  public void syncAsync() {}

  // Path loader
  private void loadPaths() {}

  // UFS loader
  private void loadMetadataFromUFS() {}


  private void updateMetadata(
      AlluxioURI path,
      @Nullable UfsStatus previousItem,
      UfsStatus[] items,
      boolean isRecursive,
      @Nullable String startAfter
  ) throws Exception {
    ReadOption.Builder readOptionBuilder = ReadOption.newBuilder();
    if (previousItem != null) {
      readOptionBuilder.setReadFrom(previousItem.getName());
    }
    String stopAt = null;
    if (items.length > 0) {
      stopAt = items[items.length- 1].getName();
    }
    Iterator<UfsStatus> ufsStatusIterator = Arrays.stream(items).iterator();
    long syncRootInodeId = mFsMaster.getFileId(path);
    RecursiveInodeIterator inodeIterator = mInodeStore.getChildrenRecursively(
        syncRootInodeId, readOptionBuilder.build(), isRecursive);
    doUpdateMetadata(path, inodeIterator, ufsStatusIterator, startAfter, stopAt);
  }

  //

  private void doUpdateMetadata(
      AlluxioURI syncRootPath,
      RecursiveInodeIterator alluxioInodeIterator,
      Iterator<UfsStatus> ufsStatusIterator,
      @Nullable String startFrom,
      @Nullable String stopAt
  ) throws Exception {
    Inode currentInode;
    while (true) {
      currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
      if (currentInode == null) {
        break;
      }
      String currentInodePath = String.join("/", alluxioInodeIterator.getCurrentURI());
      if (startFrom == null || currentInodePath.compareTo(startFrom) > 0) {
        break;
      }
    }

    UfsStatus currentUfsStatus;
    do {
      currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
    } while (startFrom != null && currentUfsStatus != null && currentUfsStatus.getName().compareTo(startFrom) <= 0);
    // Do we need to lock the current inode?

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
      // stop earlier
      String currentInodePath = null;
      if (currentInode != null) {
        // TODO write a compare function
        currentInodePath = String.join("/", alluxioInodeIterator.getCurrentURI());
        if (stopAt != null && currentInodePath.compareTo(stopAt) > 0) {
          currentInode = null;
          if (currentUfsStatus == null) {
            break;
          }
        }
      }
      System.out.println("Inode " + currentInodePath);
      if (currentUfsStatus == null) {
        System.out.println("Ufs null");
      } else {
        System.out.println("Ufs " + currentUfsStatus.getName());
      }

      Optional<Integer> comparisonResult = currentInode != null && currentUfsStatus != null
          ? Optional.of(currentInodePath.compareTo(currentUfsStatus.getName())) :
          Optional.empty();

      if (currentInode == null || (comparisonResult.isPresent() && comparisonResult.get() > 0)) {
        try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
            syncRootPath.join(currentUfsStatus.getName()), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)) {
          System.out.println("Create file " + lockedInodePath.getUri());
          // TODO create file
        }
        currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
      } else if (currentUfsStatus == null || comparisonResult.get() < 0) {
        try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
            syncRootPath.join(currentInodePath), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)) {
          System.out.println("Delete file " + lockedInodePath.getUri());
          // TODO delete file
        }
        // Remove so that its children gets skipped
        alluxioInodeIterator.skipChildrenOfTheCurrent();
        currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
      } else {
        // HDFS also fetches ACL list, which is ignored for now
        // TODO get the ufs from path resolution
        String ufsType = mMountTable.resolve(syncRootPath.join(currentInodePath))
            .acquireUfsResource().get().getUnderFSType();
        Fingerprint ufsFingerprint = Fingerprint.create(ufsType, currentUfsStatus);
        boolean containsMountPoint = mMountTable.containsMountPoint(
            syncRootPath.join(currentInodePath), true);
        UfsSyncUtils.SyncPlan syncPlan =
            UfsSyncUtils.computeSyncPlan(currentInode, ufsFingerprint, containsMountPoint);
        if (syncPlan.toUpdateMetaData() || syncPlan.toDelete() || syncPlan.toLoadMetadata()) {
          try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
              syncRootPath.join(currentInodePath), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE)) {
            if (syncPlan.toUpdateMetaData()) {
              System.out.println("Update file " + lockedInodePath.getUri());
              // TODO update file
            } else if (syncPlan.toDelete() && syncPlan.toLoadMetadata()) {
              System.out.println("Delete & Create file " + lockedInodePath.getUri());
              // TODO delete & create file
            } else {
              throw new IllegalStateException("We should never reach here.");
            }
          }
        }
        currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
        currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
      }
    }
  }
}
