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
import alluxio.collections.Pair;
import alluxio.exception.AccessControlException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.UnavailableException;
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
import alluxio.resource.CloseableIterator;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IteratorUtils;
import alluxio.util.io.PathUtils;

import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class MetadataSyncer {
  /** The FS master creating this object. */
  private final DefaultFileSystemMaster mFsMaster = null;
  private final ReadOnlyInodeStore mInodeStore = null;
  private final MountTable mMounttable = null;
  private final InodeTree mInodeTree = null;

  /**
   * Performs a metadata sync.
   * @param path the path to sync
   * @param isRecursive if children should be loaded recursively
   * @return the metadata sync result
   */
  public SyncResult sync(AlluxioURI path, boolean isRecursive) {
    System.out.println("Syncing...");
    return new SyncResult(false, 0);
  }

  private void updateMetadata(
      AlluxioURI path,
      LockedInodePath lockedInodePath,
      @Nullable UfsStatus previousItem,
      List<UfsStatus> items,
      boolean isRecursive
  ) throws Exception {
    ReadOption.Builder readOptionBuilder = ReadOption.newBuilder();
    if (previousItem != null) {
      readOptionBuilder.setReadFrom(previousItem.getName());
    }
    if (items.size() > 0) {
      readOptionBuilder.setStopAt(items.get(items.size() - 1).getName());
    }
    Iterator<UfsStatus> ufsStatusIterator = items.iterator();
    long syncRootInodeId = mFsMaster.getFileId(path);
    RecursiveInodeIterator inodeIterator = mInodeStore.getChildrenRecursively(
        syncRootInodeId, readOptionBuilder.build(), isRecursive);
    doUpdateMetadata(lockedInodePath, inodeIterator, ufsStatusIterator);
  }

  //

  private void doUpdateMetadata(
      LockedInodePath path,
      RecursiveInodeIterator alluxioInodeIterator,
      Iterator<UfsStatus> ufsStatusIterator) throws Exception {
    Inode currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
    UfsStatus currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
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
      Optional<Integer> comparisonResult = currentInode != null && currentUfsStatus != null
          ? Optional.of(currentInode.getName().compareTo(currentUfsStatus.getName())) :
          Optional.empty();

      if (currentInode == null || (comparisonResult.isPresent() && comparisonResult.get() > 0)) {
        try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
            path.getUri().join(currentUfsStatus.getName()), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE ) {
          // TODO create file
        }
        currentUfsStatus = IteratorUtils.nextOrNull(ufsStatusIterator);
      } else if (currentUfsStatus == null || comparisonResult.get() < 0) {
        try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
            alluxioInodeIterator.getCurrentURI(), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE ) {
          // TODO delete file
        }
        // Remove so that its children gets skipped
        alluxioInodeIterator.remove();
        currentInode = IteratorUtils.nextOrNull(alluxioInodeIterator);
      } else {
        // HDFS also fetches ACL list, which is ignored for now
        // TODO get the ufs from path resolution
        UnderFileSystem ufs;
        Fingerprint ufsFingerprint = Fingerprint.create("s3", currentUfsStatus);
        boolean containsMountPoint = mMounttable.containsMountPoint(
            alluxioInodeIterator.getCurrentURI(), true);
        UfsSyncUtils.SyncPlan syncPlan =
            UfsSyncUtils.computeSyncPlan(currentInode, ufsFingerprint, containsMountPoint);
        if (syncPlan.toUpdateMetaData() || syncPlan.toDelete() || syncPlan.toLoadMetadata()) {
          try (LockedInodePath lockedInodePath = mInodeTree.lockInodePath(
              alluxioInodeIterator.getCurrentURI(), InodeTree.LockPattern.WRITE_EDGE, NoopJournalContext.INSTANCE ) {
            if (syncPlan.toUpdateMetaData()) {
              // TODO update file
            } else if (syncPlan.toDelete() && syncPlan.toLoadMetadata()) {
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


// Q to discuss
// 2. how to support mount point/ nested mount? do we need to do path resolution for every file?
// 3. route some corner cases to metadata sync v1?
// 4. how to unify directory sync & inode sync? -> sync the inode itself first?
// 5. review the interface
// 7. UFS listStatus() doesn't return all metadata (S3 as an example)
// 8. we do need to add read locks or just add write locks when the inode is to be updated?
// 9. cut down the scope to shopee's ceph use case? load once/ read only/ s3/ recursively
// 10. do we need to keep a locked inode path and add/remove components along with the listing?
// 11. should we still split the metadata creation & two pointer comparison
// -> if the lock is obtained by other thread, this essentially blocks the whole for loop
}