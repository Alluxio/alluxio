/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file;

import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetAclOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * User facing interface for the Tachyon File System client APIs. File refers to any type of inode,
 * including folders. Clients should provide their own interface for reading/writing files.
 */
@PublicApi
interface TachyonFileSystemCore {

  /**
   * Creates a file.
   *
   * @param path the path of the file to create in Tachyon space
   * @param options method options
   * @return the {@link TachyonFile} instance that identifies the newly created file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  TachyonFile create(TachyonURI path, CreateOptions options) throws IOException,
      FileAlreadyExistsException, InvalidPathException, TachyonException;

  /**
   * Deletes a file or a directory.
   *
   * @param file the handler of the file to delete
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws DirectoryNotEmptyException if recursive is false and the file is a nonempty directory
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  void delete(TachyonFile file, DeleteOptions options) throws IOException,
      FileDoesNotExistException, DirectoryNotEmptyException, TachyonException;

  /**
   * Removes the file from Tachyon, but not from UFS in case it exists there.
   *
   * @param file the handler for the file
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  void free(TachyonFile file, FreeOptions options) throws IOException, FileDoesNotExistException,
      TachyonException;

  /**
   * Gets the {@link FileInfo} object that represents the metadata of a Tachyon file.
   *
   * @param file the handler for the file
   * @param options method options
   * @return the {@link FileInfo} of the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the file does not exist
   */
  FileInfo getInfo(TachyonFile file, GetInfoOptions options) throws IOException,
      FileDoesNotExistException, TachyonException;

  /**
   * If the file is a directory, returns the {@link FileInfo} of all the direct entries in it.
   * Otherwise returns the {@link FileInfo} for the file.
   *
   * @param file the handler for the file
   * @param options method options
   * @return a list of {@link FileInfo}s representing the files which are children of the given file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  List<FileInfo> listStatus(TachyonFile file, ListStatusOptions options) throws IOException,
      FileDoesNotExistException, TachyonException;

  /**
   * Loads metadata about a file in UFS to Tachyon. No data will be transferred.
   *
   * @param path the path for which to load metadat from UFS
   * @param options method options
   * @return the {@link TachyonFile} instance identifying the resulting file in Tachyon
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  TachyonFile loadMetadata(TachyonURI path, LoadMetadataOptions options)
      throws IOException, FileDoesNotExistException, TachyonException;

  /**
   * Creates a directory.
   *
   * @param path the handler for the file
   * @param options method options
   * @return true if the directory is created successfully or already existing, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  boolean mkdir(TachyonURI path, MkdirOptions options) throws IOException,
      FileAlreadyExistsException, InvalidPathException, TachyonException;

  /**
   * Mounts a UFS subtree to the given Tachyon path. The Tachyon path is expected not to exist as
   * the method creates it. This method does not transfer any data or metadata from the UFS. It
   * simply establishes the connection between the given Tachyon path and UFS path.
   *
   * @param tachyonPath a Tachyon path
   * @param ufsPath a UFS path
   * @param options method options
   * @return true if the UFS subtree was mounted successfully, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath, MountOptions options)
      throws IOException, TachyonException;

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @param options method options
   * @return a {@link TachyonFile} which acts as a file handler for the path or null if there is no
   *         file at the given path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  TachyonFile openIfExists(TachyonURI path, OpenOptions options) throws IOException,
      TachyonException;

  /**
   * Renames an existing Tachyon file to another Tachyon path in Tachyon.
   *
   * @param src the file handler for the source file
   * @param dst the path of the destination file, this path should not exist
   * @param options method options
   * @return true if successful, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  boolean rename(TachyonFile src, TachyonURI dst, RenameOptions options) throws IOException,
      FileDoesNotExistException, TachyonException;

  /**
   * Sets the acl of a file or directory.
   *
   * @param path to be set acl on
   * @param options the acl option to be set
   * @throws TachyonException if a Tachyon error occurs
   * @throws IOException an I/O error occurs
   */
  public void setAcl(TachyonURI path, SetAclOptions options) throws TachyonException,
      IOException;

  /**
   * Sets the file state.
   *
   * @param file the file handler for the file
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  void setState(TachyonFile file, SetStateOptions options) throws IOException,
      FileDoesNotExistException, TachyonException;

  /**
   * Unmounts a UFS subtree identified by the given Tachyon path. The Tachyon path match a
   * previously mounted path. The contents of the subtree rooted at this path are removed from
   * Tachyon but the corresponding UFS subtree is left untouched.
   *
   * @param tachyonPath a Tachyon path
   * @param options method options
   * @return true if the UFS subtree was unmounted successfully, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  boolean unmount(TachyonURI tachyonPath, UnmountOptions options) throws IOException,
      TachyonException;
}
