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
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

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
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  List<AlluxioURI> checkConsistency(AlluxioURI path, CheckConsistencyOptions options)
      throws AlluxioException, IOException;

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws IOException, AlluxioException;

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void createFile(AlluxioURI path, CreateFileOptions options) throws IOException, AlluxioException;

  /**
   * Marks a file as completed.
   *
   * @param path the file path
   * @param options the method options
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void completeFile(AlluxioURI path, CompleteFileOptions options)
      throws IOException, AlluxioException;

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void delete(AlluxioURI path, DeleteOptions options) throws IOException, AlluxioException;

  /**
   * Frees a file.
   *
   * @param path the path to free
   * @param options method options
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void free(AlluxioURI path, FreeOptions options) throws IOException, AlluxioException;

  /**
   * @param path the file path
   * @return the file info for the given file id
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  URIStatus getStatus(AlluxioURI path) throws IOException, AlluxioException;

  /**
   * @param path the file path
   * @return the next blockId for the file
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  long getNewBlockIdForFile(AlluxioURI path) throws IOException, AlluxioException;

  /**
   * @param path the path to list
   * @param options the listStatus options
   * @return the list of file information for the given path
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
      throws IOException, AlluxioException;

  /**
   * Loads the metadata of a file from the under file system.
   *
   * @param path the path of the file to load metadata for
   * @param options method options
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws IOException, AlluxioException;

  /**
   * Mounts the given UFS path under the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @param ufsPath the UFS path
   * @param options mount options
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException an I/O error occurs
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws AlluxioException, IOException;

  /**
   * Renames a file or a directory.
   *
   * @param src the path to rename
   * @param dst new file path
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void rename(AlluxioURI src, AlluxioURI dst) throws IOException, AlluxioException;

  /**
   * Sets the file or directory attributes.
   *
   * @param path the file or directory path
   * @param options the file or directory attribute options to be set
   * @throws IOException if an I/O error occurs
   * @throws AlluxioException if an Alluxio error occurs
   */
  void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws IOException, AlluxioException;

  /**
   * Schedules the async persistence of the given file.
   *
   * @param path the file path
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException if an I/O error occurs
   */
  void scheduleAsyncPersist(AlluxioURI path) throws AlluxioException, IOException;

  /**
   * Unmounts the given Alluxio path.
   *
   * @param alluxioPath the Alluxio path
   * @throws AlluxioException if an Alluxio error occurs
   * @throws IOException an I/O error occurs
   */
  void unmount(AlluxioURI alluxioPath) throws AlluxioException, IOException;
}
