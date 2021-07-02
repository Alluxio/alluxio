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
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidFileSizeException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.SetAclAction;
import alluxio.master.Master;
import alluxio.master.file.contexts.CheckAccessContext;
import alluxio.master.file.contexts.CheckConsistencyContext;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.contexts.WorkerHeartbeatContext;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.file.meta.PersistenceState;
import alluxio.metrics.TimeSeries;
import alluxio.security.authorization.AclEntry;
import alluxio.underfs.UfsMode;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;
import alluxio.wire.UfsInfo;
import alluxio.wire.WorkerInfo;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * The interface of file system master service.
 */
public interface FileSystemMaster extends Master {
  /**
   * Periodically clean up the under file systems.
   */
  void cleanupUfs();

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
  long getFileId(AlluxioURI path) throws AccessControlException, UnavailableException;

  /**
   * Returns the {@link FileInfo} for a given file id. This method is not user-facing but supposed
   * to be called by other internal servers (e.g., block workers, web UI).
   *
   * @param fileId the file id to get the {@link FileInfo} for
   * @return the {@link FileInfo} for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission denied
   */
  // TODO(binfan): Add permission checking for internal APIs
  FileInfo getFileInfo(long fileId)
      throws FileDoesNotExistException, AccessControlException, UnavailableException;

  /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @param context the method context
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  FileInfo getFileInfo(AlluxioURI path, GetStatusContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException, IOException;

  /**
   * Returns the persistence state for a file id.
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
   * @param context the method context
   * @return the list of {@link FileInfo}s
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  List<FileInfo> listStatus(AlluxioURI path, ListStatusContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException, IOException;

  /**
   * Enumerates given path to given batch tracker.
   * If the given path is a file, the list only
   * contains a single object. If it is a directory, the resulting list contains all direct children
   * of the directory.
   * <p>
   * This operation requires users to have READ permission on the path, and also
   * EXECUTE permission on the path if it is a directory.
   *
   * @param path the path to get the {@link FileInfo} list for
   * @param context the method context
   * @param resultStream the stream to receive individual results
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the path is invalid
   */
  void listStatus(AlluxioURI path, ListStatusContext context, ResultStream<FileInfo> resultStream)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException,
      UnavailableException, IOException;

  /**
   * @return a read-only view of the file system master
   */
  FileSystemMasterView getFileSystemMasterView();

  /**
   * Checks access to path.
   *
   * @param path the path to check access to
   * @param context the method context
   *
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  void checkAccess(AlluxioURI path, CheckAccessContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException;

  /**
   * Checks the consistency of the files and directories in the subtree under the path.
   *
   * @param path the root of the subtree to check
   * @param context the context to use for the checkConsistency method
   * @return a list of paths in Alluxio which are not consistent with the under storage
   * @throws AccessControlException if the permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   * @throws InvalidPathException if the path is invalid
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyContext context)
      throws AccessControlException, FileDoesNotExistException, InvalidPathException, IOException;

  /**
   * Completes a file. After a file is completed, it cannot be written to.
   * <p>
   * This operation requires users to have WRITE permission on the path.
   *
   * @param path the file path to complete
   * @param context the method context
   * @throws BlockInfoException if a block information exception is encountered
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws InvalidFileSizeException if an invalid file size is encountered
   * @throws FileAlreadyCompletedException if the file is already completed
   * @throws AccessControlException if permission checking fails
   */
  void completeFile(AlluxioURI path, CompleteFileContext context)
      throws BlockInfoException, FileDoesNotExistException, InvalidPathException,
      InvalidFileSizeException, FileAlreadyCompletedException, AccessControlException,
      UnavailableException;

  /**
   * Creates a file (not a directory) for a given path.
   * <p>
   * This operation requires WRITE permission on the parent of this path.
   *
   * @param path the file to create
   * @param context the method context
   * @return the file info of the created file
   * @throws InvalidPathException if an invalid path is encountered
   * @throws FileAlreadyExistsException if the file already exists
   * @throws BlockInfoException if an invalid block information is encountered
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  FileInfo createFile(AlluxioURI path, CreateFileContext context)
      throws AccessControlException, InvalidPathException, FileAlreadyExistsException,
      BlockInfoException, IOException, FileDoesNotExistException;

  /**
   * Gets a new block id for the next block of a given file to write to.
   * <p>
   * This operation requires users to have WRITE permission on the path as this API is called when
   * creating a new block for a file.
   *
   * @param path the path of the file to get the next block id for
   * @return the next block id for the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the given path is not valid
   * @throws AccessControlException if permission checking fails
   */
  long getNewBlockIdForFile(AlluxioURI path) throws FileDoesNotExistException, InvalidPathException,
      AccessControlException, UnavailableException;

  /**
   * @return a snapshot of the mount table as a mapping of Alluxio path to {@link MountPointInfo}
   */
  Map<String, MountPointInfo> getMountPointInfoSummary();

  /**
   * Gets the mount point information of an Alluxio path for display purpose.
   *
   * @param path an Alluxio path which must be a mount point
   * @return the mount point information
   */
  MountPointInfo getDisplayMountPointInfo(AlluxioURI path) throws InvalidPathException;

  /**
   * Deletes a given path.
   * <p>
   * This operation requires user to have WRITE permission on the parent of the path.
   *
   * @param path the path to delete
   * @param context method context
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void delete(AlluxioURI path, DeleteContext context)
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
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException;

  /**
   * @return absolute paths of all in-Alluxio files
   */
  List<AlluxioURI> getInAlluxioFiles() throws UnavailableException;

  /**
   * @return absolute paths of all in-memory files
   */
  List<AlluxioURI> getInMemoryFiles() throws UnavailableException;

  /**
   * Creates a directory for a given path.
   * <p>
   * This operation requires the client user to have WRITE permission on the parent of the path.
   *
   * @param path the path of the directory
   * @param context method context
   * @return the id of the created directory
   * @throws InvalidPathException when the path is invalid
   * @throws FileAlreadyExistsException when there is already a file at path
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the parent of the path does not exist and the recursive
   *         option is false
   */
  long createDirectory(AlluxioURI path, CreateDirectoryContext context)
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
   * @param context method context
   * @throws FileDoesNotExistException if a non-existent file is encountered
   * @throws InvalidPathException if an invalid path is encountered
   * @throws AccessControlException if permission checking fails
   * @throws FileAlreadyExistsException if the file already exists
   */
  void rename(AlluxioURI srcPath, AlluxioURI dstPath, RenameContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Frees or evicts all of the blocks of the file from alluxio storage. If the given file is a
   * directory, and the 'recursive' flag is enabled, all descendant files will also be freed.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to free method
   * @param context context to free method
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   * @throws UnexpectedAlluxioException if the file or directory can not be freed
   */
  // TODO(binfan): throw a better exception rather than UnexpectedAlluxioException. Currently
  // UnexpectedAlluxioException is thrown because we want to keep backwards compatibility with
  // clients of earlier versions prior to 1.5. If a new exception is added, it will be converted
  // into RuntimeException on the client.
  void free(AlluxioURI path, FreeContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnexpectedAlluxioException, IOException;

  /**
   * Gets the path of a file with the given id.
   *
   * @param fileId the id of the file to look up
   * @return the path of the file
   * @throws FileDoesNotExistException raise if the file does not exist
   */
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
   * Mounts a UFS path onto an Alluxio path.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to mount to
   * @param ufsPath the UFS path to mount
   * @param context the mount context
   * @throws FileAlreadyExistsException if the path to be mounted to already exists
   * @throws FileDoesNotExistException if the parent of the path to be mounted to does not exist
   * @throws InvalidPathException if an invalid path is encountered
   * @throws AccessControlException if the permission check fails
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountContext context)
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
   * Update properties of an Alluxio mount point.
   * <p>
   * This operation requires users to have WRITE permission on the parent
   * of the Alluxio path.
   *
   * @param alluxioPath the Alluxio path to update, must be a mount point
   * @param context the mount context
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws InvalidPathException if the given path is not a mount point
   * @throws AccessControlException if the permission check fails
   */
  void updateMount(AlluxioURI alluxioPath, MountContext context)
      throws FileAlreadyExistsException, FileDoesNotExistException, InvalidPathException,
      IOException, AccessControlException;

  /**
   * Sets the ACL for a path.
   *
   * @param path the path to set attribute for
   * @param action the set action to perform
   * @param entries the list of ACL entries for setting ACL
   * @param context the context for setting ACL
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclContext context)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException,
      IOException;

  /**
   * Sets the file attribute.
   * <p>
   * This operation requires users to have WRITE permission on the path. In
   * addition, the client user must be a super user when setting the owner, and must be a super user
   * or the owner when setting the group or permission.
   *
   * @param path the path to set attribute for
   * @param options master operation context
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the given path is invalid
   */
  void setAttribute(AlluxioURI path, SetAttributeContext options)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException,
      IOException;

  /**
   * Schedules a file for async persistence.
   *
   * @param path the path of the file for persistence
   * @param context the schedule async persistence context
   */
  void scheduleAsyncPersistence(AlluxioURI path, ScheduleAsyncPersistenceContext context)
      throws AlluxioException, UnavailableException;

  /**
   * Update the operation mode for the given ufs path under one or more mount points.
   *
   * @param ufsPath the physical ufs path
   * @param ufsMode the ufs operation mode
   * @throws InvalidPathException if ufs path is not used by any mount point
   * @throws InvalidArgumentException if arguments for the method are invalid
   */
  void updateUfsMode(AlluxioURI ufsPath, UfsMode ufsMode)
      throws InvalidPathException, InvalidArgumentException, UnavailableException,
      AccessControlException;

  /**
   * Checks the integrity of the inodes with respect to the blocks of the system.
   *
   * @param repair if true, will attempt to repair the state of the system when inconsistencies are
   *               discovered
   * @throws UnavailableException if the repair attempt fails
   */
  void validateInodeBlocks(boolean repair) throws UnavailableException;

  /**
   * Instructs a worker to persist the files.
   * <p>
   * Needs WRITE permission on the list of files.
   *
   * @param workerId the id of the worker that heartbeats
   * @param persistedFiles the files that persisted on the worker
   * @param context the method context
   * @return the command for persisting the blocks of a file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path corresponding to the file id is invalid
   * @throws AccessControlException if permission checking fails
   */
  FileSystemCommand workerHeartbeat(long workerId, List<Long> persistedFiles,
      WorkerHeartbeatContext context)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException, IOException;

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  List<WorkerInfo> getWorkerInfoList() throws UnavailableException;

  /**
   * @return the list of sync path
   */
  List<SyncPointInfo> getSyncPathList() throws UnavailableException, AccessControlException;

  /**
   * starts active sync on a specified alluxioURI.
   *
   * @param alluxioURI sync point which is a valid path in Alluxio namespace
   * @throws UnavailableException
   * @throws InvalidPathException
   * @throws AccessControlException
   */
  void startSync(AlluxioURI alluxioURI) throws IOException, InvalidPathException,
      AccessControlException, ConnectionFailedException;

  /**
   * stops active sync on a specific syncpoint.
   *
   * @param alluxioURI alluxio path that has been added as a sync point
   * @throws UnavailableException
   * @throws InvalidPathException
   * @throws AccessControlException
   */
  void stopSync(AlluxioURI alluxioURI) throws IOException, InvalidPathException,
      AccessControlException;

  /**
   * Starts a batch sync with a list of changed files passed in.
   * If no files are passed in, sync the entire path.
   * @param path the path to sync
   * @param changedFiles collection of files that are changed under the path to sync
   * @param executorService executor for executing parallel syncs
   */
  void activeSyncMetadata(AlluxioURI path, Collection<AlluxioURI> changedFiles,
      ExecutorService executorService) throws IOException;

  /**
   * Journal the active sync transaction id so that we can restart more efficiently.
   *
   * @param txId transaction id
   * @param mountId mount id
   * @return true if successfully recorded in the journal
   */
  boolean recordActiveSyncTxid(long txId, long mountId);

  /**
   * Get the total inode count.
   *
   * @return inode count
   */
  long getInodeCount();

  /**
   * @return the time series data stored by the master
   */
  List<TimeSeries> getTimeSeries();

  /**
   * Reverse path resolve a ufs uri to an Alluxio path.
   *
   * @param ufsUri ufs uri
   * @return alluxio path
   */
  AlluxioURI reverseResolve(AlluxioURI ufsUri) throws InvalidPathException;

  /**
   * @return the owner of the root inode, null if the inode tree is not initialized
   */
  String getRootInodeOwner();

  /**
   * @return the list of thread identifiers that are waiting and holding the state lock
   */
  List<String> getStateLockSharedWaitersAndHolders();
}
