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
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.master.Master;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.options.MountInfo;
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.FileSystemCommand;
<<<<<<< HEAD
import alluxio.thrift.FileSystemCommandOptions;
import alluxio.thrift.FileSystemMasterClientService;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.thrift.PersistCommandOptions;
import alluxio.thrift.PersistFile;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.ListOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.util.IdUtils;
import alluxio.util.SecurityUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
=======
>>>>>>> master
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interface of file system master.
 */
public interface FileSystemMaster extends Master {
  /**
   * @return the status of the startup consistency check and inconsistent paths if it is complete
   */
  StartupConsistencyCheck getStartupConsistencyCheck();

  /**
   * Returns the file id for a given path. If the given path does not exist in Alluxio, the method
   * attempts to load it from UFS.
   * <p>
   * This operation requires users to have READ permission of the path.
   *
   * @param path the path to get the file id for
   * @return the file id for a given path, or -1 if there is no file at that path
   * @throws AccessControlException if permission checking fails
   */
  long getFileId(AlluxioURI path) throws AccessControlException;

  /**
   * Returns the {@link FileInfo} for a given file id. This method is not user-facing but supposed
   * to be called by other internal servers (e.g., block workers, lineage master, web UI).
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  // TODO(binfan): Add permission checking for internal APIs
  FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException;

  /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  // TODO(peis): Add an option not to load metadata.
  FileInfo getFileInfo(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * Returns the persistence state for a file id. This method is used by the lineage master.
   *
   * @param fileId the file id
   * @return the {@link PersistenceState} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   */
  // TODO(binfan): Add permission checking for internal APIs
  PersistenceState getPersistenceState(long fileId) throws FileDoesNotExistException;

  /**
   * Returns a list of {@link FileInfo} for a given path. If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * <p>
   * This operation requires users to have READ permission on the path, and also
   * EXECUTE permission on the path if it is a directory.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @param listStatusOptions the {@link alluxio.master.file.options.ListStatusOptions}
   * @return the list of {@link FileInfo}s
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  List<FileInfo> listStatus(AlluxioURI path, ListStatusOptions listStatusOptions)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException;

  /**
   * @return a read-only view of the file system master
   */
  FileSystemMasterView getFileSystemMasterView();

  /**
   * Checks the consistency of the files and directories in the subtree under the path.
   *
   * @param path the root of the subtree to check
   * @param options the options to use for the checkConsistency method
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws AccessControlException if the permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   * @throws InvalidPathException if the path is invalid
   * @throws IOException if an error occurs interacting with the under storage
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException;

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * This operation requires users to have WRITE permission on the path.
   *
   * @param path the file path to complete
   * @param options the method options
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws AccessControlException if permission checking fails
   */
  void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException;

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * This operation requires WRITE permission on the parent of this path.
   *
   * @param path the file to create
   * @param options method options
   * @return the id of the created file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information is encountered
   * @throws IOException if the creation fails
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   * option is false
   */
  long createFile(AlluxioURI path, CreateFileOptions options)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException;

  /**
   * Reinitializes the blocks of an existing open file.
   *
   * @param path the path to the file
   * @param blockSizeBytes the new block size
   * @param ttl the ttl
   * @param ttlAction action to take after Ttl expiry
   * @return the file id
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the path does not exist
   */
  // Used by lineage master
  long reinitializeFile(AlluxioURI path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, FileDoesNotExistException;

  /**
   * Gets a new block id for the next block of a given file to write to.
   * <p>
   * This operation requires users to have WRITE permission on the path as
   * this API is called when creating a new block for a file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   * @throws AccessControlException if permission checking fails
   */
  long getNewBlockIdForFile(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * @return a copy of the current mount table
   */
  Map<String, MountInfo> getMountTable();

  /**
   * @return the number of files and directories
   */
  int getNumberOfPaths();

  /**
   * @return the number of pinned files and directories
   */
  int getNumberOfPinnedFiles();

  /**
   * Deletes a given path.
   * <p>
   * This operation requires user to have WRITE permission on the parent of the path.
   *
   * @param path the path to delete
   * @param options method options
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void delete(AlluxioURI path, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, InvalidPathException,
<<<<<<< HEAD
      AccessControlException {
    Metrics.DELETE_PATHS_OPS.inc();
    // Delete should lock the parent to remove the child inode.
    try (JournalContext journalContext = createJournalContext();
        LockedInodePath inodePath = mInodeTree
            .lockFullInodePath(path, InodeTree.LockMode.WRITE_PARENT)) {
      mPermissionChecker.checkParentPermission(Mode.Bits.WRITE, inodePath);
      mMountTable.checkUnderWritableMountPoint(path);
      deleteAndJournal(inodePath, options, journalContext);
    }
  }

  /**
   * Deletes a given path.
   * <p>
   * Writes to the journal.
   *
   * @param inodePath the path to delete
   * @param deleteOptions the method options
   * @param journalContext the journal context
   * @throws InvalidPathException if the path is invalid
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  private void deleteAndJournal(LockedInodePath inodePath, DeleteOptions deleteOptions,
      JournalContext journalContext) throws InvalidPathException, FileDoesNotExistException,
      IOException, DirectoryNotEmptyException {
    Inode<?> inode = inodePath.getInode();
    long fileId = inode.getId();
    long opTimeMs = System.currentTimeMillis();
    deleteInternal(inodePath, false, opTimeMs, deleteOptions);
    DeleteFileEntry deleteFile = DeleteFileEntry.newBuilder().setId(fileId)
        .setRecursive(deleteOptions.isRecursive()).setOpTimeMs(opTimeMs).build();
    appendJournalEntry(JournalEntry.newBuilder().setDeleteFile(deleteFile).build(), journalContext);
  }

  /**
   * @param entry the entry to use
   */
  private void deleteFromEntry(DeleteFileEntry entry) {
    Metrics.DELETE_PATHS_OPS.inc();
    // Delete should lock the parent to remove the child inode.
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(entry.getId(), InodeTree.LockMode.WRITE_PARENT)) {
      deleteInternal(inodePath, true, entry.getOpTimeMs(), DeleteOptions.defaults()
          .setRecursive(entry.getRecursive()).setAlluxioOnly(entry.getAlluxioOnly()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convenience method for avoiding {@link DirectoryNotEmptyException} when calling
   * {@link #deleteInternal(LockedInodePath, boolean, long, DeleteOptions)}.
   *
   * @param inodePath the {@link LockedInodePath} to delete
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if the fileId is for the root directory
   * @throws IOException if an I/O error is encountered
   */
  private void deleteRecursiveInternal(LockedInodePath inodePath, boolean replayed, long opTimeMs,
      DeleteOptions deleteOptions) throws FileDoesNotExistException, IOException,
      InvalidPathException {
    try {
      deleteInternal(inodePath, replayed, opTimeMs, deleteOptions);
    } catch (DirectoryNotEmptyException e) {
      throw new IllegalStateException(
          "deleteInternal should never throw DirectoryNotEmptyException when recursive is true", e);
    }
  }

  /**
   * Implements file deletion.
   *
   * @param inodePath the file {@link LockedInodePath}
   * @param replayed whether the operation is a result of replaying the journal
   * @param opTimeMs the time of the operation
   * @param deleteOptions the method optitions
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws IOException if an I/O error is encountered
   * @throws InvalidPathException if the specified path is the root
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   */
  private void deleteInternal(LockedInodePath inodePath, boolean replayed, long opTimeMs,
      DeleteOptions deleteOptions) throws FileDoesNotExistException, IOException,
      DirectoryNotEmptyException, InvalidPathException {
    // TODO(jiri): A crash after any UFS object is deleted and before the delete operation is
    // journaled will result in an inconsistency between Alluxio and UFS.
    if (!inodePath.fullPathExists()) {
      return;
    }
    Inode<?> inode = inodePath.getInode();
    if (inode == null) {
      return;
    }
    boolean recursive = deleteOptions.isRecursive();
    boolean alluxioOnly = deleteOptions.isAlluxioOnly();
    if (inode.isDirectory() && !recursive && ((InodeDirectory) inode).getNumberOfChildren() > 0) {
      // inode is nonempty, and we don't want to delete a nonempty directory unless recursive is
      // true
      throw new DirectoryNotEmptyException(ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE,
          inode.getName());
    }
    if (mInodeTree.isRootId(inode.getId())) {
      // The root cannot be deleted.
      throw new InvalidPathException(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    }

    // Number of inodes deleted
    long deletedCount = 0;

    // Inodes for un-persisted paths to be deleted
    List<Inode<?>> delInodes = new ArrayList<>();

    // AlluxioURIs and inodes for persisted paths
    HashMap<AlluxioURI, Inode<?>> recursiveUFSDeletes = new HashMap<>();
    HashMap<AlluxioURI, Inode<?>> nonRecursiveUFSDeletes = new HashMap<>();

    if (!inode.isPersisted()) {
      delInodes.add(inode);
    } else if (!replayed) {
      if (inode.isFile()) {
        nonRecursiveUFSDeletes.put(inodePath.getUri(), inode);
      } else {
        // TODO(adit): put behind a check option
        if (isUFSDeleteSafe((InodeDirectory) inodePath.getInode(), inodePath.getUri())) {
          recursiveUFSDeletes.put(inodePath.getUri(), inode);
        } else {
          nonRecursiveUFSDeletes.put(inodePath.getUri(), inode);
        }
      }
    }

    try (InodeLockList lockList = mInodeTree.lockDescendants(inodePath, InodeTree.LockMode.WRITE)) {
      // Traverse inodes top-down
      for (Inode descendant : lockList.getInodes()) {
        if (!descendant.isPersisted()) {
          delInodes.add(descendant);
        } else if (!replayed) {
          AlluxioURI currentPath = mInodeTree.getPath(descendant);
          if (descendant.isFile()) {
            nonRecursiveUFSDeletes.put(currentPath, inode);
          } else {
            // TODO(adit): put behind a check option
            // Check if immediate children in UFS have inodes
            if (isUFSDeleteSafe((InodeDirectory) descendant, currentPath)) {
              // Directory is a candidate for recursive deletes
              recursiveUFSDeletes.put(currentPath, descendant);
            } else {
              nonRecursiveUFSDeletes.put(currentPath, descendant);
              // Invalidate ancestor directories if not a mount point
              if (!mMountTable.isMountPoint(currentPath)) {
                for (AlluxioURI ancestor = currentPath.getParent(); ancestor != null
                    && recursiveUFSDeletes.containsKey(ancestor); ancestor = ancestor.getParent()) {
                  nonRecursiveUFSDeletes.put(ancestor, recursiveUFSDeletes.get(ancestor));
                  recursiveUFSDeletes.remove(ancestor);
                }
              }
            }
          }
        }
      }

      // Remove entries covered by a recursive delete
      for (Map.Entry<AlluxioURI, Inode<?>> entry : nonRecursiveUFSDeletes.entrySet()) {
        AlluxioURI currentPath = entry.getKey();
        if (recursiveUFSDeletes.containsKey(currentPath.getParent())) {
          nonRecursiveUFSDeletes.remove(currentPath);
        }
      }
      for (Map.Entry<AlluxioURI, Inode<?>> entry : recursiveUFSDeletes.entrySet()) {
        AlluxioURI currentPath = entry.getKey();
        if (!recursiveUFSDeletes.containsKey(currentPath.getParent())) {
          recursiveUFSDeletes.remove(currentPath);
        }
      }

      TempInodePathForDescendant tempInodePath = new TempInodePathForDescendant(inodePath);
      // We go through each inode, removing it from its parent set and from mDelInodes. If it's a
      // file, we deal with the checkpoints and blocks as well.
      for (int i = delInodes.size() - 1; i >= 0; i--) {
        Inode<?> delInode = delInodes.get(i);
        // the path to delInode for getPath should already be locked.
        AlluxioURI alluxioUriToDel = mInodeTree.getPath(delInode);
        tempInodePath.setDescendant(delInode, alluxioUriToDel);

        // TODO(jiri): What should the Alluxio behavior be when a UFS delete operation fails?
        // Currently, it will result in an inconsistency between Alluxio and UFS.
        if (!replayed && delInode.isPersisted()) {
          try {
            // If this is a mount point, we have deleted all the children and can unmount it
            // TODO(calvin): Add tests (ALLUXIO-1831)
            if (mMountTable.isMountPoint(alluxioUriToDel)) {
              unmountInternal(alluxioUriToDel);
            } else {
              // Delete the file in the under file system.
              MountTable.Resolution resolution = mMountTable.resolve(alluxioUriToDel);
              String ufsUri = resolution.getUri().toString();
              UnderFileSystem ufs = resolution.getUfs();
              boolean failedToDelete = false;
              if (!alluxioOnly) {
                if (delInode.isFile()) {
                  if (!ufs.deleteFile(ufsUri)) {
                    failedToDelete = ufs.isFile(ufsUri);
                    if (!failedToDelete) {
                      LOG.warn("The file to delete does not exist in ufs: {}", ufsUri);
                    }
                  }
                } else {
                  // TODO(adit): change this to recursive or non-recursive depending on map
                  if (!ufs.deleteDirectory(ufsUri, alluxio.underfs.options.DeleteOptions
                      .defaults().setRecursive(true))) {
                    failedToDelete = ufs.isDirectory(ufsUri);
                    if (!failedToDelete) {
                      LOG.warn("The directory to delete does not exist in ufs: {}", ufsUri);
                    }
                  }
                }
                if (failedToDelete) {
                  LOG.error("Failed to delete {} from the under filesystem", ufsUri);
                  throw new IOException(ExceptionMessage.DELETE_FAILED_UFS.getMessage(ufsUri));
                }
              }
            }
          } catch (InvalidPathException e) {
            LOG.warn(e.getMessage());
          }
        }

        if (delInode.isFile()) {
          // Remove corresponding blocks from workers and delete metadata in master.
          mBlockMaster.removeBlocks(((InodeFile) delInode).getBlockIds(), true /* delete */);
        }

        mInodeTree.deleteInode(tempInodePath, opTimeMs);
      }
    }

    // TODO(adit): get delete count
    Metrics.PATHS_DELETED.inc(deletedCount);
  }

  /**
   * Check if immediate children of directory are in sync with UFS.
   *
   * @param inode directory to check
   * @param path of directory to to check
   * @return true is contents of directory match
   */
  private boolean isUFSDeleteSafe(InodeDirectory inode, AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, IOException {
    if (!inode.isPersisted()) {
      return false;
    }

    AlluxioURI alluxioUri = mInodeTree.getPath(inode);
    MountTable.Resolution resolution = mMountTable.resolve(alluxioUri);
    String ufsUri = resolution.getUri().toString();
    UnderFileSystem ufs = resolution.getUfs();
    UnderFileStatus[] ufsChildren =
        ufs.listStatus(ufsUri, ListOptions.defaults().setRecursive(false));

    for (UnderFileStatus child : ufsChildren) {
      AlluxioURI expectedPath = path.join(child.getName());
      boolean found = false;
      for (Inode<?> inodeChild : inode.getChildren()) {
        if (expectedPath.equals(mInodeTree.getPath(inodeChild))) {
          found = true;
        }
      }
      if (!found) {
        return false;
      }
    }
    return true;
  }
=======
      AccessControlException;
>>>>>>> master

  /**
   * Gets the {@link FileBlockInfo} for all blocks of a file. If path is a directory, an exception
   * is thrown.
   * <p>
<<<<<<< HEAD
   * This operation requires the client user to have {@link Mode.Bits#READ} permission on the the
   * path.
=======
   * This operation requires the client user to have READ permission on the the path.
>>>>>>> master
   *
   * @param path the path to get the info for
   * @return a list of {@link FileBlockInfo} for all the blocks of the given path
   * @throws FileDoesNotExistException if the file does not exist or path is a directory
   * @throws InvalidPathException if the path of the given file is invalid
   * @throws AccessControlException if permission checking fails
   */
  List<FileBlockInfo> getFileBlockInfoList(AlluxioURI path)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * @return absolute paths of all in memory files
   */
  List<AlluxioURI> getInMemoryFiles();

  /**
   * Creates a directory for a given path.
   * <p>
   * This operation requires the client user to have WRITE permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param options method options
   * @return the id of the created directory
   * @throws InvalidPathException when the path is invalid
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws IOException if a non-Alluxio related exception occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  long createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws InvalidPathException, FileAlreadyExistsException, IOException, AccessControlException,
      FileDoesNotExistException;

  /**
   * Renames a file to a destination.
   * <p>
   * This operation requires users to have WRITE permission on the parent of the src path, and
   * WRITE permission on the parent of the dst path.
   *
   * @param srcPath the source path to rename
   * @param dstPath the destination path to rename the file to
   * @param options method options
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   * @throws FileAlreadyExistsException if the file already exists
   */
  void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to free
   * @param options options to free
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // TODO(binfan): throw a better exception rather than UnexpectedAlluxioException. Currently
  // UnexpectedAlluxioException is thrown because we want to keep backwards compatibility with
  // clients of earlier versions prior to 1.5. If a new exception is added, it will be converted
  // into RuntimeException on the client.
  void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException;

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  AlluxioURI getPath(long fileId) throws FileDoesNotExistException;

  /**
   * @return the set of inode ids which are pinned
   */
  Set<Long> getPinIdList();

  /**
   * @return the ufs address for this master
   */
  String getUfsAddress();

  /**
   * @return the white list
   */
  List<String> getWhiteList();

  /**
   * @return all the files lost on the workers
   */
  List<Long> getLostFiles();

  /**
   * Reports a file as lost.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  void reportLostFile(long fileId) throws FileDoesNotExistException;

  /**
   * Loads metadata for the object identified by the given path from UFS into Alluxio.
   * <p>
   * This operation requires users to have WRITE permission on the path
   * and its parent path if path is a file, or WRITE permission on the
   * parent path if path is a directory.
   *
   * @param path the path for which metadata should be loaded
   * @param options the load metadata options
   * @return the file id of the loaded path
   * @throws BlockInfoException if an invalid block size is encountered
   * @throws FileDoesNotExistException if there is no UFS path
   * @throws InvalidPathException if invalid path is encountered
   * @throws InvalidFileSizeException if invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if permission checking fails
   */
  long loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, IOException, AccessControlException;

  /**
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param options the mount options
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Unmounts a UFS path previously mounted onto an Alluxio path.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to unmount, must be a mount point
   * @throws FileDoesNotExistException if the path to be mounted does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws IOException if an I/O error occurs
   * @throws AccessControlException if the permission check fails
   */
  void unmount(AlluxioURI alluxioPath)
      throws FileDoesNotExistException, InvalidPathException, IOException, AccessControlException;

  /**
   * Resets a file. It first free the whole file, and then reinitializes it.
   *
   * @param fileId the id of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid for the id of the file
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // Currently used by Lineage Master
  // TODO(binfan): Add permission checking for internal APIs
  void resetFile(long fileId)
      throws UnexpectedAlluxioException, FileDoesNotExistException, InvalidPathException,
      AccessControlException;

  /**
   * Sets the file attribute.
   * <p>
   * This operation requires users to have WRITE permission on the path. In
   * addition, the client user must be a super user when setting the owner, and must be a super user
   * or the owner when setting the group or permission.
   *
   * @param path the path to set attribute for
   * @param options attributes to be set, see {@link SetAttributeOptions}
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException;

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path of the file for persistence
   * @throws AlluxioException if scheduling fails
   */
  void scheduleAsyncPersistence(AlluxioURI path) throws AlluxioException;

  /**
   * Instructs a worker to persist the files.
   * <p>
   * Needs WRITE permission on the list of files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
  FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException;

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList();
}
