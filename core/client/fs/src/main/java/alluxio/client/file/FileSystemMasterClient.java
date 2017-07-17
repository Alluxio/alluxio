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
import alluxio.MasterClient;
import alluxio.client.file.options.CheckConsistencyOptions;
import alluxio.client.file.options.CompleteFileOptions;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.wire.MountPointInfo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import javax.security.auth.Subject;

/**
 * A client to use for interacting with a file system master.
 */
public interface FileSystemMasterClient extends MasterClient {

  /**
   * Factory for {@link FileSystemMasterClient}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Factory method for {@link FileSystemMasterClient}.
     *
     * @param masterAddress the master address
     * @return a new {@link FileSystemMasterClient} instance
     */
    public static FileSystemMasterClient create(InetSocketAddress masterAddress) {
      return create(null, masterAddress);
    }

    /**
     * Factory method for {@link FileSystemMasterClient}.
     *
     * @param subject the parent subject
     * @param masterAddress the master address
     * @return a new {@link FileSystemMasterClient} instance
     */
    public static FileSystemMasterClient create(Subject subject, InetSocketAddress masterAddress) {
      return RetryHandlingFileSystemMasterClient.create(subject, masterAddress);
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
      throws IOException;

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws AlreadyExistsException if the directory already exists
   */
  void createDirectory(AlluxioURI path, CreateDirectoryOptions options) throws IOException;

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @throws AlreadyExistsException if the file already exists
   */
  void createFile(AlluxioURI path, CreateFileOptions options) throws IOException;

  /**
   * Marks a file as completed.
   *
   * @param path the file path
   * @param options the method options
   */
  void completeFile(AlluxioURI path, CompleteFileOptions options) throws IOException;

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete
   * @param options method options
   */
  void delete(AlluxioURI path, DeleteOptions options) throws IOException;

  /**
   * Frees a file.
   *
   * @param path the path to free
   * @param options method options
   * @throws NotFoundException if the path does not exist
   */
  void free(AlluxioURI path, FreeOptions options) throws IOException;

  /**
   * @param path the file path
   * @param options the getStatus options
   * @return the file info for the given file id
   * @throws NotFoundException if the path does not exist
   */
  URIStatus getStatus(AlluxioURI path, GetStatusOptions options) throws IOException;

  /**
   * @param path the file path
   * @return the next blockId for the file
   */
  long getNewBlockIdForFile(AlluxioURI path) throws IOException;

  /**
   * @param path the path to list
   * @param options the listStatus options
   * @return the list of file information for the given path
   * @throws NotFoundException if the path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options) throws IOException;

  /**
   * Loads the metadata of a file from the under file system.
   *
   * @param path the path of the file to load metadata for
   * @param options method options
   * @deprecated since version 1.1 and will be removed in version 2.0
   * @throws NotFoundException if the path does not exist
   */
  @Deprecated
  void loadMetadata(AlluxioURI path, LoadMetadataOptions options) throws IOException;

  /**
   * Mounts the given UFS path under the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @param ufsPath the UFS path
   * @param options mount options
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options) throws IOException;

  /**
   * Lists all mount points and their corresponding under storage addresses.
   *
   * @return a map from String to {@link MountPointInfo}
   */
  Map<String, MountPointInfo> getMountTable() throws IOException;

  /**
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @throws NotFoundException if the path does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst) throws IOException;

  /**
   * Sets the file or directory attributes.
   *
   * @param path the file or directory path
   * @param options the file or directory attribute options to be set
   * @throws NotFoundException if the path does not exist
   */
  void setAttribute(AlluxioURI path, SetAttributeOptions options) throws IOException;

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   */
  void scheduleAsyncPersist(AlluxioURI path) throws IOException;

  /**
   * Unmounts the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   */
  void unmount(AlluxioURI alluxioPath) throws IOException;
}
