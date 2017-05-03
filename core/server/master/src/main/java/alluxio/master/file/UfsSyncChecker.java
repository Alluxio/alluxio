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

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class to check if the contents of the under storage is in-sync with the master.
 */
@NotThreadSafe
public final class UfsSyncChecker {

  /** This manages the file system mount points. */
  private final MountTable mMountTable;

  /**
   * Create a new instance of {@link UfsSyncChecker}.
   *
   * @param mountTable to resolve path in under storage
   */
  public UfsSyncChecker(MountTable mountTable) {
    mMountTable = mountTable;
  }
  /**
   * Check if immediate children of directory are in sync with UFS.
   *
   * @param inode read-locked directory to check
   * @param path of directory to to check
   * @return true is contents of directory match
   */
  public boolean isUFSDeleteSafe(InodeDirectory inode, AlluxioURI alluxioUri)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    Preconditions.checkArgument(inode.isPersisted());

    MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
    String ufsUri = resolution.getUri().toString();
    UnderFileSystem ufs = resolution.getUfs();
    UnderFileStatus[] ufsChildren =
        ufs.listStatus(ufsUri, ListOptions.defaults().setRecursive(false));
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
      if (ufsChildren[ufsPos].getName().equals(inodeChildren[inodePos].getName())) {
        ufsPos++;
      }
      inodePos++;
    }
    return ufsPos == ufsChildren.length;
  }
}
