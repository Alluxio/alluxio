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
import alluxio.annotation.SuppressFBWarnings;
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
import java.util.function.Predicate;
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
  // TODO(jiacheng): use AlluxioURI as key to avoid toString()
  private final Map<String, UfsStatus[]> mListedDirectories;

  /** This manages the file system mount points. */
  private final MountTable mMountTable;
  /** This manages inode tree state. */
  private final ReadOnlyInodeStore mInodeStore;

  /** Directories in sync with the UFS. */
  private final Map<AlluxioURI, InodeDirectory> mSyncedDirectories = new HashMap<>();

  private static final Predicate<UfsStatus> IS_TEMP_FILE =
      (status) -> PathUtils.isTemporaryFileName(status.getName());
  private static final Predicate<UfsStatus> CONTAINS_SEPARATOR =
      (status) -> status.getName().contains(AlluxioURI.SEPARATOR);

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
  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  public void checkDirectory(InodeDirectory inode, AlluxioURI alluxioUri)
      throws InvalidPathException, IOException {
    Preconditions.checkArgument(inode.isPersisted());
    // TODO(jiacheng): try to improve this, especially string sort can be O(N*L)
    //  and many substring() are used
    UfsStatus[] ufsChildren = getChildrenInUFS(alluxioUri);
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
    // Move Outside the for loop
    String prefix = PathUtils.normalizePath(ufsUri.toString(), AlluxioURI.SEPARATOR);
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      AlluxioURI curUri = ufsUri;
      while (curUri != null) {
        if (mListedDirectories.containsKey(curUri.toString())) {
          UfsStatus[] childrenStatuses = mListedDirectories.get(curUri.toString());
          return trimAndTranslate(childrenStatuses, curUri, prefix);
        }

        // If the URI has not been listed, walk up the tree
        // TODO(jiacheng): this can loop many times until ufs / is reached
        curUri = curUri.getParent();
      }

      // List the UFS if the parent dirs have not been listed
      UfsStatus[] ufsAllChildren =
          ufs.listStatus(ufsUri.toString(), ListOptions.defaults().setRecursive(true));
      System.out.println(Arrays.toString(ufsAllChildren));
      // Assumption: multiple mounted UFSs cannot have the same ufsUri
      if (ufsAllChildren == null) {
        // Memoize an empty dir as an empty array to avoid NPE
        mListedDirectories.put(ufsUri.toString(), EMPTY_CHILDREN);
        return EMPTY_CHILDREN;
      }

      // Memoize the listed result
      // We do not filter the temp files, in order to avoid one copy
      mListedDirectories.put(ufsUri.toString(), ufsAllChildren);

      return trimTempAndIndirect(ufsAllChildren);
    }
  }

  /**
   * Remove indirect children from children list returned from recursive listing.
   * Also filters out temporary files.
   *
   * @param children list from recursive listing
   * @return trimmed list, null if input is null
   */
  // TODO(jiacheng): Compare performance of vanilla java for-loop
  private UfsStatus[] trimTempAndIndirect(UfsStatus[] children) {
    return Arrays.stream(children)
        .filter((child) -> !IS_TEMP_FILE.test(child) && !CONTAINS_SEPARATOR.test(child))
        .toArray(UfsStatus[]::new);
  }

  private UfsStatus[] trimAndTranslate(UfsStatus[] children, AlluxioURI baseUri, String prefix) {
    List<UfsStatus> childrenList = new ArrayList<>();
    for (UfsStatus childStatus : children) {
      if (IS_TEMP_FILE.test(childStatus)) {
        continue;
      }
      // Find only the children under the target path
      String childPath = PathUtils.concatPath(baseUri, childStatus.getName());
      // TODO(jiacheng): check if the part after prefix has separator
      if (childPath.startsWith(prefix) && childPath.length() > prefix.length()) {
        System.out.println("Before copy: " + childStatus);
        UfsStatus newStatus = childStatus.copy();
        newStatus.setName(childPath.substring(prefix.length()));

        // If the path is indirect, ignore
        if (CONTAINS_SEPARATOR.test(newStatus)) {
          continue;
        }

        childrenList.add(newStatus);
        System.out.println("After copy: " + newStatus);
      }
    }

    // TODO(jiacheng): This copy looks unavoidable because of the translation
    return childrenList.toArray(new UfsStatus[0]);
  }
}
