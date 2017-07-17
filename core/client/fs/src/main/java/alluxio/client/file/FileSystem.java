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
import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.ExistsOptions;
import alluxio.client.file.options.FreeOptions;
import alluxio.client.file.options.GetStatusOptions;
import alluxio.client.file.options.ListStatusOptions;
import alluxio.client.file.options.LoadMetadataOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.options.RenameOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.client.file.options.UnmountOptions;
import alluxio.client.lineage.LineageContext;
import alluxio.client.lineage.LineageFileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.wire.MountPointInfo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Basic file system interface supporting metadata operations and data operations. Developers
 * should not implement this class but extend the default implementation provided by {@link
 * BaseFileSystem} instead. This ensures any new methods added to the interface will be provided
 * by the default implementation.
 */
@PublicApi
public interface FileSystem {

  /**
   * Factory for {@link FileSystem}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    public static FileSystem get() {
      if (Configuration.getBoolean(PropertyKey.USER_LINEAGE_ENABLED)) {
        return LineageFileSystem.get(FileSystemContext.INSTANCE, LineageContext.INSTANCE);
      }
      return BaseFileSystem.get(FileSystemContext.INSTANCE);
    }

    public static FileSystem get(FileSystemContext context) {
      if (Configuration.getBoolean(PropertyKey.USER_LINEAGE_ENABLED)) {
        return LineageFileSystem.get(context, LineageContext.INSTANCE);
      }
      return BaseFileSystem.get(context);
    }
  }

  /**
   * Convenience method for {@link #createDirectory(AlluxioURI, CreateDirectoryOptions)} with
   * default options.
   *
   * @param path the path of the directory to create in Alluxio space
   * @throws FileAlreadyExistsException if there is already a file or directory at the given path
   * @throws InvalidPathException if the path is invalid
   */
  void createDirectory(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Creates a directory.
   *
   * @param path the path of the directory to create in Alluxio space
   * @param options options to associate with this operation
   * @throws FileAlreadyExistsException if there is already a file or directory at the given path
   * @throws InvalidPathException if the path is invalid
   */
  void createDirectory(AlluxioURI path, CreateDirectoryOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #createFile(AlluxioURI, CreateFileOptions)} with default options.
   *
   * @param path the path of the file to create in Alluxio space
   * @return a {@link FileOutStream} which will write data to the newly created file
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  FileOutStream createFile(AlluxioURI path)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Creates a file.
   *
   * @param path the path of the file to create in Alluxio space
   * @param options options to associate with this operation
   * @return a {@link FileOutStream} which will write data to the newly created file
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  FileOutStream createFile(AlluxioURI path, CreateFileOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #delete(AlluxioURI, DeleteOptions)} with default options.
   *
   * @param path the path to delete in Alluxio space
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the path is a nonempty directory
   */
  void delete(AlluxioURI path)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Deletes a file or a directory.
   *
   * @param path the path to delete in Alluxio space
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the path is a nonempty directory
   */
  void delete(AlluxioURI path, DeleteOptions options)
      throws DirectoryNotEmptyException, FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #exists(AlluxioURI, ExistsOptions)} with default options.
   *
   * @param path the path in question
   * @return true if the path exists, false otherwise
   * @throws InvalidPathException if the path is invalid
   */
  boolean exists(AlluxioURI path) throws InvalidPathException, IOException, AlluxioException;

  /**
   * Checks whether a path exists in Alluxio space.
   *
   * @param path the path in question
   * @param options options to associate with this operation
   * @return true if the path exists, false otherwise
   * @throws InvalidPathException if the path is invalid
   */
  boolean exists(AlluxioURI path, ExistsOptions options)
      throws InvalidPathException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #free(AlluxioURI, FreeOptions)} with default options.
   *
   * @param path the path to free in Alluxio space
   * @throws FileDoesNotExistException if the given path does not exist
   */
  void free(AlluxioURI path) throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Evicts any data under the given path from Alluxio space, but does not delete the data from the
   * UFS. The metadata will still be present in Alluxio space after this operation.
   *
   * @param path the path to free in Alluxio space
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   */
  void free(AlluxioURI path, FreeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #getStatus(AlluxioURI, GetStatusOptions)} with default options.
   *
   * @param path the path to obtain information about
   * @return the {@link URIStatus} of the file
   * @throws FileDoesNotExistException if the path does not exist
   */
  URIStatus getStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Gets the {@link URIStatus} object that represents the metadata of an Alluxio path.
   *
   * @param path the path to obtain information about
   * @param options options to associate with this operation
   * @return the {@link URIStatus} of the file
   * @throws FileDoesNotExistException if the path does not exist
   */
  URIStatus getStatus(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #listStatus(AlluxioURI, ListStatusOptions)} with default options.
   *
   * @param path the path to list information about
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * If the path is a directory, returns the {@link URIStatus} of all the direct entries in it.
   * Otherwise returns a list with a single {@link URIStatus} element for the file.
   *
   * @param path the path to list information about
   * @param options options to associate with this operation
   * @return a list of {@link URIStatus}s containing information about the files and directories
   *         which are children of the given path
   * @throws FileDoesNotExistException if the given path does not exist
   */
  List<URIStatus> listStatus(AlluxioURI path, ListStatusOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #loadMetadata(AlluxioURI, LoadMetadataOptions)} with default
   * options.
   *
   * @param path the path for which to load metadata from UFS
   * @throws FileDoesNotExistException if the given path does not exist
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  void loadMetadata(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Loads metadata about a path in the UFS to Alluxio. No data will be transferred.
   *
   * @param path the path for which to load metadata from UFS
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given path does not exist
   * @deprecated since version 1.1 and will be removed in version 2.0
   */
  @Deprecated
  void loadMetadata(AlluxioURI path, LoadMetadataOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #mount(AlluxioURI, AlluxioURI, MountOptions)} with default
   * options.
   *
   * @param alluxioPath an Alluxio path to mount the data to
   * @param ufsPath a UFS path to mount the data from
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath) throws IOException, AlluxioException;

  /**
   * Mounts a UFS subtree to the given Alluxio path. The Alluxio path is expected not to exist as
   * the method creates it. If the path already exists, a {@link AlluxioException} will be thrown.
   * This method does not transfer any data or metadata from the UFS. It simply establishes the
   * connection between the given Alluxio path and UFS path.
   *
   * @param alluxioPath an Alluxio path to mount the data to
   * @param ufsPath a UFS path to mount the data from
   * @param options options to associate with this operation
   */
  void mount(AlluxioURI alluxioPath, AlluxioURI ufsPath, MountOptions options)
      throws IOException, AlluxioException;

  /**
   * Lists all mount points and their corresponding under storage addresses.
   * @return a map from String to {@link MountPointInfo}
   */
  Map<String, MountPointInfo> getMountTable() throws IOException, AlluxioException;

  /**
   * Convenience method for {@link #openFile(AlluxioURI, OpenFileOptions)} with default options.
   *
   * @param path the file to read from
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException if the given file does not exist
   */
  FileInStream openFile(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Opens a file for reading.
   *
   * @param path the file to read from
   * @param options options to associate with this operation
   * @return a {@link FileInStream} for the given path
   * @throws FileDoesNotExistException if the given file does not exist
   */
  FileInStream openFile(AlluxioURI path, OpenFileOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #rename(AlluxioURI, AlluxioURI, RenameOptions)} with default
   * options.
   *
   * @param src the path of the source, this must already exist
   * @param dst the path of the destination, this path should not exist
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Renames an existing Alluxio path to another Alluxio path in Alluxio. This operation will be
   * propagated in the underlying storage if the path is persisted.
   *
   * @param src the path of the source, this must already exist
   * @param dst the path of the destination, this path should not exist
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void rename(AlluxioURI src, AlluxioURI dst, RenameOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #setAttribute(AlluxioURI, SetAttributeOptions)} with default
   * options.
   *
   * @param path the path to set attributes for
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void setAttribute(AlluxioURI path)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Sets any number of a path's attributes, such as TTL and pin status.
   *
   * @param path the path to set attributes for
   * @param options options to associate with this operation
   * @throws FileDoesNotExistException if the given file does not exist
   */
  void setAttribute(AlluxioURI path, SetAttributeOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Convenience method for {@link #unmount(AlluxioURI, UnmountOptions)} with default options.
   *
   * @param path an Alluxio path, this must be a mount point
   */
  void unmount(AlluxioURI path) throws IOException, AlluxioException;

  /**
   * Unmounts a UFS subtree identified by the given Alluxio path. The Alluxio path match a
   * previously mounted path. The contents of the subtree rooted at this path are removed from
   * Alluxio but the corresponding UFS subtree is left untouched.
   *
   * @param path an Alluxio path, this must be a mount point
   * @param options options to associate with this operation
   */
  void unmount(AlluxioURI path, UnmountOptions options) throws IOException, AlluxioException;
}
