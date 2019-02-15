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
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectory;
import alluxio.master.file.meta.MountTable;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class to check if the contents of the under storage is in-sync with the master.
 */
@NotThreadSafe
public final class UfsSyncChecker {
  private static final Logger LOG = LoggerFactory.getLogger(UfsSyncChecker.class);

  /** Empty array for a directory with no children. */
  private static final UfsStatus[] EMPTY_CHILDREN = new UfsStatus[0];

  /** UFS directories for which list was called. */
  private Map<String, UfsStatus[]> mListedDirectories;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;
  /** This manages inode tree state. */
  private final ReadOnlyInodeStore mInodeStore;

  /** Directories in sync with the UFS. */
  private Map<AlluxioURI, InodeDirectory> mSyncedDirectories = new HashMap<>();

  /**
   * Create a new instance of {@link UfsSyncChecker}.
   *
   * @param mountTable to resolve path in under storage
   * @param inodeStore to look up inode children
   */
  public UfsSyncChecker(MountTable mountTable, ReadOnlyInodeStore inodeStore) {
    mListedDirectories = new HashMap<>();
    mMountTable = mountTable;
    mInodeStore = inodeStore;
  }

  /**
   * Check if immediate children of directory are in sync with UFS.
   *
   * @param inode read-locked directory to check
   * @param alluxioUri path of directory to to check
   */
  public void checkDirectory(InodeDirectory inode, AlluxioURI alluxioUri)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    Preconditions.checkArgument(inode.isPersisted());
    UfsStatus[] ufsChildren = getChildrenInUFS(alluxioUri);
    // Filter out temporary files
    ufsChildren = Arrays.stream(ufsChildren)
        .filter(ufsStatus -> !PathUtils.isTemporaryFileName(ufsStatus.getName()))
        .toArray(UfsStatus[]::new);
    Arrays.sort(ufsChildren, Comparator.comparing(UfsStatus::getName));
    Inode[] alluxioChildren = Iterables.toArray(mInodeStore.getChildren(inode), Inode.class);
    Arrays.sort(alluxioChildren);
    int ufsPos = 0;
    for (Inode alluxioInode : alluxioChildren) {
      if (ufsPos >= ufsChildren.length) {
        break;
      }
      String ufsName = ufsChildren[ufsPos].getName();
      if (ufsName.endsWith(AlluxioURI.SEPARATOR)) {
        ufsName = ufsName.substring(0, ufsName.length() - 1);
      }
      if (ufsName.equals(alluxioInode.getName())) {
        ufsPos++;
      }
    }

    if (ufsPos == ufsChildren.length) {
      // Directory is in sync
      mSyncedDirectories.put(alluxioUri, inode);
    } else {
      // Invalidate ancestor directories if not a mount point
      AlluxioURI currentPath = alluxioUri;
      while (currentPath.getParent() != null && !mMountTable.isMountPoint(currentPath)
          && mSyncedDirectories.containsKey(currentPath.getParent())) {
        mSyncedDirectories.remove(currentPath.getParent());
        currentPath = currentPath.getParent();
      }
      LOG.debug("Ufs file {} does not match any file in Alluxio", ufsChildren[ufsPos]);
    }
  }

  /**
   * Based on directories for which
   * {@link UfsSyncChecker#checkDirectory(InodeDirectory, AlluxioURI)} was called, this
   * method returns whether any un-synced entries were found.
   *
   * @param alluxioUri path of directory to check
   * @return true, if in sync; false, otherwise
   */
  public boolean isDirectoryInSync(AlluxioURI alluxioUri) {
    return mSyncedDirectories.containsKey(alluxioUri);
  }

  /**
   * Get the children in under storage for given alluxio path.
   *
   * @param alluxioUri alluxio path
   * @return the list of children in under storage
   * @throws InvalidPathException if aluxioUri is invalid
   * @throws IOException if a non-alluxio error occurs
   */
  private UfsStatus[] getChildrenInUFS(AlluxioURI alluxioUri)
      throws InvalidPathException, IOException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
    AlluxioURI ufsUri = resolution.getUri();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      AlluxioURI curUri = ufsUri;
      while (curUri != null) {
        if (mListedDirectories.containsKey(curUri.toString())) {
          List<UfsStatus> childrenList = new ArrayList<>();
          for (UfsStatus childStatus : mListedDirectories.get(curUri.toString())) {
            String childPath = PathUtils.concatPath(curUri, childStatus.getName());
            String prefix = PathUtils.normalizePath(ufsUri.toString(), AlluxioURI.SEPARATOR);
            if (childPath.startsWith(prefix) && childPath.length() > prefix.length()) {
              UfsStatus newStatus = childStatus.copy();
              newStatus.setName(childPath.substring(prefix.length()));
              childrenList.add(newStatus);
            }
          }
          return trimIndirect(childrenList.toArray(new UfsStatus[childrenList.size()]));
        }
        curUri = curUri.getParent();
      }
      UfsStatus[] children =
          ufs.listStatus(ufsUri.toString(), ListOptions.defaults().setRecursive(true));
      // Assumption: multiple mounted UFSs cannot have the same ufsUri
      if (children == null) {
        return EMPTY_CHILDREN;
      }
      mListedDirectories.put(ufsUri.toString(), children);
      return trimIndirect(children);
    }
  }

  /**
   * Remove indirect children from children list returned from recursive listing.
   *
   * @param children list from recursive listing
   * @return trimmed list, null if input is null
   */
  private UfsStatus[] trimIndirect(UfsStatus[] children) {
    List<UfsStatus> childrenList = new ArrayList<>();
    for (UfsStatus child : children) {
      int index = child.getName().indexOf(AlluxioURI.SEPARATOR);
      if (index < 0 || index == child.getName().length()) {
        childrenList.add(child);
      }
    }
    return childrenList.toArray(new UfsStatus[childrenList.size()]);
  }
}
