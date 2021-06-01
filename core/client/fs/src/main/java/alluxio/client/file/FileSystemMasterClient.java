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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.Client;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.CheckAccessPOptions;
import alluxio.grpc.CheckConsistencyPOptions;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.ScheduleAsyncPersistencePOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UpdateUfsModePOptions;
import alluxio.master.MasterClientContext;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SyncPointInfo;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A client to use for interacting with a file system master.
 */
public interface FileSystemMasterClient extends Client {

  /**
   * Factory for {@link FileSystemMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link FileSystemMasterClient}.
     *
     * @param conf master client configuration
     * @return a new {@link FileSystemMasterClient} instance
     */
    public static FileSystemMasterClient create(MasterClientContext conf) {
      return new RetryHandlingFileSystemMasterClient(conf);
    }
  }

  /**
   * Check access to a path.
   *
   * @param path the path to check
   * @param options method options
   * @throws alluxio.exception.AccessControlException if the access is denied
   */
  void checkAccess(AlluxioURI path, CheckAccessPOptions options)
      throws AlluxioStatusException;

  /**
   * Checks the consistency of Alluxio metadata against the under storage for all files and
   * directories in a given subtree.
   *
   * @param path the root of the subtree to check
   * @param options method options
   * @return a list of inconsistent files and directories
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyPOptions options)
      throws AlluxioStatusException;

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws AlreadyExistsException if the directory already exists
   */
  void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws AlluxioStatusException;

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @throws AlreadyExistsException if the file already exists
   * @return the uri status of the newly created file
   */
  URIStatus createFile(AlluxioURI path, CreateFilePOptions options) throws AlluxioStatusException;

  /**
   * Marks a file as completed.
   *
   * @param path the file path
   * @param options the method options
   */
  void completeFile(AlluxioURI path, CompleteFilePOptions options) throws AlluxioStatusException;

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete
   * @param options method options
   */
  void delete(AlluxioURI path, DeletePOptions options) throws AlluxioStatusException;

  /**
   * Frees a file.
   *
   * @param path the path to free
   * @param options method options
   * @throws NotFoundException if the path does not exist
   */
  void free(AlluxioURI path, FreePOptions options) throws AlluxioStatusException;

  /**
   * @param fileId a file id
   * @return the file path for the given file id
   */
  String getFilePath(long fileId) throws AlluxioStatusException;

  /**
   * @param path the file path
   * @param options the getStatus options
   * @return the file info for the given file id
   * @throws NotFoundException if the path does not exist
   */
  URIStatus getStatus(AlluxioURI path, GetStatusPOptions options) throws AlluxioStatusException;

  /**
   * @param path the file path
   * @return the next blockId for the file
   */
  long getNewBlockIdForFile(AlluxioURI path) throws AlluxioStatusException;

  /**
   * get the list of paths that are currently being actively synced.
   *
   * @return the list of paths
   */
  List<SyncPointInfo> getSyncPathList() throws AlluxioStatusException;

  /**
   * Performs a specific action on each {@code URIStatus} in the result of {@link #listStatus}.
   * This method is preferred when iterating over directories with a large number of files or
   * sub-directories inside. The caller can proceed with partial result without waiting for all
   * result returned.
   *
   * @param path the path to list information about
   * @param options options to associate with this operation
   * @param action action to apply on each {@code URIStatus}
   * @throws NotFoundException if the path does not exist
   */
  void iterateStatus(AlluxioURI path, ListStatusPOptions options,
      Consumer<? super URIStatus> action) throws AlluxioStatusException;

  /**
   * @param path the path to list
   * @param options the listStatus options
   * @return the list of file information for the given path
   * @throws NotFoundException if the path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws AlluxioStatusException;

  /**
   * Mounts the given UFS path under the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @param ufsPath the UFS path
   * @param options mount options
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountPOptions options)
      throws AlluxioStatusException;

  /**
   * Updates options of a mount point for the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @param options mount options
   */
  void updateMount(AlluxioURI alluxioPath, MountPOptions options) throws AlluxioStatusException;

  /**
   * Lists all mount points and their corresponding under storage addresses.
   *
   * @return a map from String to {@link MountPointInfo}
   */
  Map<String, MountPointInfo> getMountTable() throws AlluxioStatusException;

  /**
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @throws NotFoundException if the path does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst) throws AlluxioStatusException;

  /**
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @param options rename options
   * @throws NotFoundException if the path does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options) throws AlluxioStatusException;

  /**
   * Reverse resolve a ufs uri.
   *
   * @param ufsUri the ufs uri
   * @return the alluxio path for the ufsUri
   * @throws AlluxioStatusException
   */
  AlluxioURI reverseResolve(AlluxioURI ufsUri) throws AlluxioStatusException;

  /**
   * Sets the ACL for a path.
   *
   * @param path the file or directory path
   * @param action the set action to perform
   * @param entries the ACL entries to use
   * @param options the options for setting ACL
   * @throws NotFoundException if the path does not exist
   */
  void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclPOptions options)
      throws AlluxioStatusException;

  /**
   * Sets the file or directory attributes.
   *
   * @param path the file or directory path
   * @param options the file or directory attribute options to be set
   * @throws NotFoundException if the path does not exist
   */
  void setAttribute(AlluxioURI path, SetAttributePOptions options) throws AlluxioStatusException;

  /**
   * Start the active syncing process for a specified path.
   *
   * @param path the file or directory to be synced
   * @throws AlluxioStatusException
   */
  void startSync(AlluxioURI path) throws AlluxioStatusException;

  /**
   * Stop the active syncing process for a specified path.
   *
   * @param path the file or directory to stop syncing
   * @throws AlluxioStatusException
   */
  void stopSync(AlluxioURI path) throws AlluxioStatusException;

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   * @param options options to use when scheduling the persist
   */
  void scheduleAsyncPersist(AlluxioURI path, ScheduleAsyncPersistencePOptions options)
      throws AlluxioStatusException;

  /**
   * Unmounts the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   */
  void unmount(AlluxioURI alluxioPath) throws AlluxioStatusException;

  /**
   * Updates the operation mode for the given ufs path. The path is required to be the scheme and
   * authority only. For example, to update the mode for under storage at hdfs://ns/folder1
   * specify the argument as hdfs://ns/. Note: the mode for any other mounted under storage which
   * shares the prefix (such as hdfs://ns/folder2) is also updated.
   *
   * @param ufsUri the ufs path
   * @param options the options to update ufs operation mode
   */
  void updateUfsMode(AlluxioURI ufsUri, UpdateUfsModePOptions options)
      throws AlluxioStatusException;

  /**
   * @return the state lock waiters and holders thread identifiers
   */
  List<String> getStateLockHolders() throws AlluxioStatusException;
}
