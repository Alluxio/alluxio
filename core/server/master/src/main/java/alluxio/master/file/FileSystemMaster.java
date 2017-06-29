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
import alluxio.master.file.options.CheckConsistencyOptions;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.UfsInfo;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.MountPointInfo;
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
   * @param options the {@link GetStatusOptions}
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  FileInfo getFileInfo(AlluxioURI path, GetStatusOptions options)
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
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
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
  Map<String, MountPointInfo>  getMountTable();

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
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void delete(AlluxioURI path, DeleteOptions options)
      throws IOException, FileDoesNotExistException, DirectoryNotEmptyException,
      InvalidPathException, AccessControlException;

  /**
   * Gets the {@link FileBlockInfo} for all blocks of a file. If path is a directory, an exception
   * is thrown.
   * <p>
   * This operation requires the client user to have READ permission on the the path.
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
   * @param mountId the mount id to query
   * @return the ufs information for the given mount id
   */
  UfsInfo getUfsInfo(long mountId);

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
   * @throws InvalidPathException if the given path is not a mount point
   * @throws AccessControlException if the permission check fails
   */
  void unmount(AlluxioURI alluxioPath) throws FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

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
      throws FileDoesNotExistException, AccessControlException, InvalidPathException,
      IOException;

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path of the file for persistence
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
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      IOException;

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList();
}
