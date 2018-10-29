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
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAclOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UpdateUfsModeOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.master.MasterClientConfig;
import alluxio.security.authorization.AclEntry;
import alluxio.wire.MountPointInfo;
import alluxio.wire.SetAclAction;

import java.util.List;
import java.util.Map;

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
    public static FileSystemMasterClient create(MasterClientConfig conf) {
      return new RetryHandlingFileSystemMasterClient(conf);
    }
  }

  /**
   * Checks the consistency of Alluxio metadata against the under storage for all files and
   * directories in a given subtree.
   *
   * @param path the root of the subtree to check
   * @param options method options
   * @return a list of inconsistent files and directories
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AlluxioStatusException;

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws AlreadyExistsException if the directory already exists
   */
  void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws AlluxioStatusException;

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @throws AlreadyExistsException if the file already exists
   */
  void createFile(AlluxioURI path, CreateFileOptions options) throws AlluxioStatusException;

  /**
   * Marks a file as completed.
   *
   * @param path the file path
   * @param options the method options
   */
  void completeFile(AlluxioURI path, CompleteFileOptions options) throws AlluxioStatusException;

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete
   * @param options method options
   */
  void delete(AlluxioURI path, DeleteOptions options) throws AlluxioStatusException;

  /**
   * Frees a file.
   *
   * @param path the path to free
   * @param options method options
   * @throws NotFoundException if the path does not exist
   */
  void free(AlluxioURI path, FreeOptions options) throws AlluxioStatusException;

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
   * @param path the path to list
   * @param options the listStatus options
   * @return the list of file information for the given path
   * @throws NotFoundException if the path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
      throws AlluxioStatusException;

  /**
   * Mounts the given UFS path under the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @param ufsPath the UFS path
   * @param options mount options
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws AlluxioStatusException;

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
  void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options) throws AlluxioStatusException;

  /**
   * Sets the ACL for a path.
   *
   * @param path the file or directory path
   * @param action the set action to perform
   * @param entries the ACL entries to use
   * @param options the options for setting ACL
   * @throws NotFoundException if the path does not exist
   */
  void setAcl(AlluxioURI path, SetAclAction action, List<AclEntry> entries, SetAclOptions options)
      throws AlluxioStatusException;

  /**
   * Sets the file or directory attributes.
   *
   * @param path the file or directory path
   * @param options the file or directory attribute options to be set
   * @throws NotFoundException if the path does not exist
   */
  void setAttribute(AlluxioURI path, SetAttributeOptions options) throws AlluxioStatusException;

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   */
  void scheduleAsyncPersist(AlluxioURI path) throws AlluxioStatusException;

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
  void updateUfsMode(AlluxioURI ufsUri, UpdateUfsModeOptions options) throws AlluxioStatusException;
}
