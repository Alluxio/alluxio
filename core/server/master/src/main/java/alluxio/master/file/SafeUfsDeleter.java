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
import alluxio.collections.Pair;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.grpc.DeletePOptions;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.MountTable;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Helper class for deleting persisted entries from the UFS.
 */
public final class SafeUfsDeleter implements UfsDeleter {
  private static final Logger LOG = LoggerFactory.getLogger(SafeUfsDeleter.class);

  private final MountTable mMountTable;
  private final AlluxioURI mRootPath;
  private UfsSyncChecker mUfsSyncChecker;

  /**
   * Creates a new instance of {@link SafeUfsDeleter}.
   *
   * @param mountTable the mount table
   * @param inodeStore the inode store
   * @param inodes sub-tree being deleted (any node should appear before descendants)
   * @param deleteOptions delete options
   */
  public SafeUfsDeleter(MountTable mountTable, ReadOnlyInodeStore inodeStore,
      List<Pair<AlluxioURI, LockedInodePath>> inodes, DeletePOptions deleteOptions)
      throws IOException, FileDoesNotExistException, InvalidPathException {
    mMountTable = mountTable;
    // Root of sub-tree occurs before any of its descendants
    mRootPath = inodes.get(0).getFirst();
    if (!deleteOptions.getUnchecked() && !deleteOptions.getAlluxioOnly()) {
      mUfsSyncChecker = new UfsSyncChecker(mMountTable, inodeStore);
      for (Pair<AlluxioURI, LockedInodePath> inodePair : inodes) {
        AlluxioURI alluxioUri = inodePair.getFirst();
        Inode inode = inodePair.getSecond().getInodeOrNull();
        // Mount points are not deleted recursively as we need to preserve the directory itself
        if (inode != null && inode.isPersisted() && inode.isDirectory()
            && !mMountTable.isMountPoint(alluxioUri)) {
          mUfsSyncChecker.checkDirectory(inode.asDirectory(), alluxioUri);
        }
      }
    }
  }

  @Override
  public void delete(AlluxioURI alluxioUri, Inode inode)
      throws IOException, InvalidPathException {
    MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
    String ufsUri = resolution.getUri().toString();
    try (CloseableResource<UnderFileSystem> ufsResource = resolution.acquireUfsResource()) {
      UnderFileSystem ufs = ufsResource.get();
      AlluxioURI parentUri = alluxioUri.getParent();
      if (!isRecursiveDeleteSafe(parentUri)) {
        // Parent will not recursively delete, so delete this inode individually
        if (inode.isFile()) {
          if (!ufs.deleteExistingFile(ufsUri)) {
            if (ufs.isFile(ufsUri)) {
              throw new IOException(ExceptionMessage.DELETE_FAILED_UFS_FILE.getMessage());
            } else {
              LOG.warn("The file to delete does not exist in ufs: {}", ufsUri);
            }
          }
        } else {
          if (isRecursiveDeleteSafe(alluxioUri)) {
            DeleteOptions options =
                DeleteOptions.defaults().setRecursive(true);
            if (!ufs.deleteExistingDirectory(ufsUri, options)) {
              // TODO(adit): handle partial failures of recursive deletes
              if (ufs.isDirectory(ufsUri)) {
                throw new IOException(ExceptionMessage.DELETE_FAILED_UFS_DIR.getMessage());
              } else {
                LOG.warn("The directory to delete does not exist in ufs: {}", ufsUri);
              }
            }
          } else {
            LOG.warn("The directory cannot be deleted from the ufs as it is not in sync: {}",
                ufsUri);
            throw new IOException(ExceptionMessage.DELETE_FAILED_UFS_NOT_IN_SYNC.getMessage());
          }
        }
      }
    }
  }

  /**
   * Check if recursively deleting from the UFS is "safe".
   *
   * @param alluxioUri Alluxio path to delete
   * @return true, if path can be deleted recursively from UFS; false, otherwise
   */
  private boolean isRecursiveDeleteSafe(@Nullable AlluxioURI alluxioUri) {
    if (alluxioUri == null || !alluxioUri.toString().startsWith(mRootPath.toString())) {
      // Path is not part of sub-tree being deleted
      return false;
    }
    if (mUfsSyncChecker == null) {
      // Delete is unchecked
      return true;
    }
    return mUfsSyncChecker.isDirectoryInSync(alluxioUri);
  }
}
