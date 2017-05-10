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
import alluxio.underfs.UnderFileStatus;
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

  /** UFS directories for which list was called. */
  private Map<String, UnderFileStatus[]> mListedDirectories;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

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
   * @return true is contents of directory match
   */
  public boolean isUFSDeleteSafe(InodeDirectory inode, AlluxioURI alluxioUri)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    Preconditions.checkArgument(inode.isPersisted());

    UnderFileStatus[] ufsChildren = getChildrenInUFS(alluxioUri);
    // Empty directories are not persisted
    if (ufsChildren == null) {
      return true;
    }
    // Sort-merge compare
    Arrays.sort(ufsChildren, new Comparator<UnderFileStatus>() {
      @Override
      public int compare(UnderFileStatus a, UnderFileStatus b) {
        return a.getName().compareTo(b.getName());
      }
    });
    int numInodeChildren = inode.getChildren().size();
    Inode[] inodeChildren = new Inode[numInodeChildren];
    inodeChildren = inode.getChildren().toArray(inodeChildren);
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
      if (ufsName.equals(inodeChildren[inodePos].getName())) {
        ufsPos++;
      }
      inodePos++;
    }
    return ufsPos == ufsChildren.length;
  }

  /**
   * Get the children in under storage for given alluxio path.
   *
   * @param alluxioUri alluxio path
   * @return the list of children in under storage
   * @throws InvalidPathException if aluxioUri is invalid
   * @throws IOException if a non-alluxio error occurs
   */
  private UnderFileStatus[] getChildrenInUFS(AlluxioURI alluxioUri)
      throws InvalidPathException, IOException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
    AlluxioURI ufsUri = resolution.getUri();
    UnderFileSystem ufs = resolution.getUfs();

    AlluxioURI curUri = ufsUri;
    while (curUri != null) {
      if (mListedDirectories.containsKey(curUri.toString())) {
        List<UnderFileStatus> childrenList = new LinkedList<>();
        for (UnderFileStatus child : mListedDirectories.get(curUri.toString())) {
          String childPath = PathUtils.concatPath(curUri, child.getName());
          String prefix = PathUtils.normalizePath(ufsUri.toString(), AlluxioURI.SEPARATOR);
          if (childPath.startsWith(prefix) && childPath.length() > prefix.length()) {
            childrenList.add(new UnderFileStatus(childPath.substring(prefix.length()),
                child.isDirectory()));
          }
        }
        return trimIndirect(childrenList.toArray(new UnderFileStatus[childrenList.size()]));
      }
      curUri = curUri.getParent();
    }
    UnderFileStatus[] children =
        ufs.listStatus(ufsUri.toString(), ListOptions.defaults().setRecursive(true));
    // Assumption: multiple mounted UFSs cannot have the same ufsUri
    if (children != null) {
      mListedDirectories.put(ufsUri.toString(), children);
    }
    return trimIndirect(children);
  }

  /**
   * Remove indirect children from children list returned from recursive listing.
   *
   * @param children list from recursive listing
   * @return trimmed list, null if input is null
   */
  private UnderFileStatus[] trimIndirect(UnderFileStatus[] children) {
    if (children == null) {
      return null;
    }
    List<UnderFileStatus> childrenList = new LinkedList<>();
    for (UnderFileStatus child : children) {
      int index = child.getName().indexOf(AlluxioURI.SEPARATOR);
      if (index < 0 || index == child.getName().length()) {
        childrenList.add(child);
      }
    }
    return childrenList.toArray(new UnderFileStatus[childrenList.size()]);
  }
}
