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
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.ListOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class to check if the contents of the under storage is in-sync with the master.
 */
@NotThreadSafe
public final class UfsSyncChecker {

  /** Empty array for a directory with no children. */
  private static final UfsStatus[] EMPTY_CHILDREN = new UfsStatus[0];

  /** UFS directories for which list was called. */
  private Map<String, UfsStatus[]> mListedDirectories;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /** Directories in sync with the UFS. */
  private Map<AlluxioURI, Inode<?>> mSyncedDirectories = new HashMap<>();

  /**
   * Create a new instance of {@link UfsSyncChecker}.
   *
   * @param mountTable to resolve path in under storage
   */
  public UfsSyncChecker(MountTable mountTable) {
    mListedDirectories = new HashMap<>();
    mMountTable = mountTable;
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
    Arrays.sort(ufsChildren, new Comparator<UfsStatus>() {
      @Override
      public int compare(UfsStatus a, UfsStatus b) {
        return a.getName().compareTo(b.getName());
      }
    });
    int numInodeChildren = inode.getChildren().size();
    Inode[] inodeChildren = inode.getChildren().toArray(new Inode[numInodeChildren]);
    Arrays.sort(inodeChildren, new Comparator<Inode>() {
      @Override
      public int compare(Inode a, Inode b) {
        return a.getName().compareTo(b.getName());
      }
    });
    int ufsPos = 0;
    int inodePos = 0;
    while (ufsPos < ufsChildren.length && inodePos < numInodeChildren) {
      String ufsName = ufsChildren[ufsPos].getName();
      if (ufsName.endsWith(AlluxioURI.SEPARATOR)) {
        ufsName = ufsName.substring(0, ufsName.length() - 1);
      }
      if (PathUtils.isTemporaryFileName(ufsName)) {
        // Check if Alluxio is aware of permanent filename
        ufsName = PathUtils.getPermanentFileName(ufsName);
      }
      if (ufsName.equals(inodeChildren[inodePos].getName())) {
        ufsPos++;
      }
      inodePos++;
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
    }
  }

  /**
   * Based on directories for which
   * {@link UfsSyncChecker#checkDirectory(InodeDirectory, AlluxioURI)} was called, this method
   * returns whether any un-synced entries were found.
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
    UnderFileSystem ufs = resolution.getUfs();

    AlluxioURI curUri = ufsUri;
    while (curUri != null) {
      if (mListedDirectories.containsKey(curUri.toString())) {
        List<UfsStatus> childrenList = new LinkedList<>();
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

  /**
   * Remove indirect children from children list returned from recursive listing.
   *
   * @param children list from recursive listing
   * @return trimmed list, null if input is null
   */
  private UfsStatus[] trimIndirect(UfsStatus[] children) {
    List<UfsStatus> childrenList = new LinkedList<>();
    for (UfsStatus child : children) {
      int index = child.getName().indexOf(AlluxioURI.SEPARATOR);
      if (index < 0 || index == child.getName().length()) {
        childrenList.add(child);
      }
    }
    return childrenList.toArray(new UfsStatus[childrenList.size()]);
  }
}
