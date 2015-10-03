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
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.client.file.options.WaitCompletedOptions;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
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
   * @return the file id that identifies the newly created file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  // (andrea) why doesn't this method return a file.TachyonFile instance instead?
  long create(TachyonURI path, CreateOptions options) throws IOException,
      FileAlreadyExistsException, InvalidPathException, TachyonException;

  /**
   * Deletes a file or a directory.
   *
   * @param file the handler of the file to delete
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  void delete(TachyonFile file, DeleteOptions options) throws IOException,
      FileDoesNotExistException, TachyonException;

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
   * @param file the handler for the file.
   * @param options method options
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  FileInfo getInfo(TachyonFile file, GetInfoOptions options) throws IOException, TachyonException;

  /**
   * If the file is a directory, returns the {@link FileInfo} of all the direct entries in it.
   * Otherwise returns the {@link FileInfo} for the file.
   *
   * @param file the handler for the file
   * @param options method options
   * @return a list of FileInfos representing the files which are children of the given file
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
   * @return the file id of the resulting file in Tachyon
   * @throws IOException if a non-Tachyon exception occurs
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  long loadMetadata(TachyonURI path, LoadMetadataOptions options)
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
   * @return a TachyonFile which acts as a file handler for the path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws InvalidPathException if the path is invalid
   * @throws TachyonException if an unexpected tachyon exception is thrown
   */
  TachyonFile open(TachyonURI path, OpenOptions options) throws IOException, InvalidPathException,
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
   * Sets the state of a file.
   *
   * @param file the file handler for the file to pin
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


  /**
   * Wait for a file to be marked as completed.
   *
   * The calling thread will block for <i>at most</i>
   * {@link WaitCompletedOptions#getTimeout()} time units (as specified via {@link
   * WaitCompletedOptions#getTunit()}) or until the TachyonFile is reported as complete by the
   * master. The method will return
   * the last known completion status of the file (hence, false only if the method
   * has timed out).  A negative value on the timeout parameter will make the thread block
   * forever; a zero value will make it check just once and return.
   *
   * Note that the file whose uri is specified, might not exist at the moment this method this
   * call. The method will deliberately block anyway for the specified amount of time, waiting
   * for the file to be created and eventually completed. Note also that the file might be moved
   * or deleted while it is waited upon. In such cases the method will throw the a
   * {@link TachyonException} with the appropriate {@link tachyon.exception.TachyonExceptionType}
   *
   * @param f the file whose completion status is to be waitd for.
   * @param options parameters for this waitCompleted call
   * @return true if the file is complete when this method returns and false
   * if the method timed out before the file was complete.
   *
   * @throws IOException in case there are problems contacting the Tachyonmaster
   * for the file status
   * @throws TachyonException if a Tachyon Exception occurs
   * @throws InterruptedException if the thread receives an interrupt while
   * waiting for file completion
   */
  boolean waitCompleted(TachyonURI uri, WaitCompletedOptions options)
    throws IOException, TachyonException, InterruptedException;
}
