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
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * User facing interface for the Tachyon File System client APIs. File refers to any type of inode,
 * including folders. Clients should provide their own interface for reading/writing files.
 */
@PublicApi
interface TachyonFileSystemCore {

  /**
   * Deletes a file. If the file is a folder, its contents will be deleted recursively.
   *
   * @param file the handler of the file to delete.
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws IOException if the master is unable to delete the file
   */
  void delete(TachyonFile file) throws IOException, FileDoesNotExistException;

  /**
   * Removes the file from Tachyon Storage. The underlying under storage system file will not be
   * removed. If the file is a folder, its contents will be freed recursively.
   *
   * @param file the handler for the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if the master is unable to free the file for some other reason
   */
  void free(TachyonFile file) throws IOException, FileDoesNotExistException;

  /**
   * Gets the FileInfo object that represents the Tachyon file
   *
   * @param file the handler for the file.
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if the master cannot retrieve the file's metadata for some other reason
   */
  FileInfo getInfo(TachyonFile file) throws IOException;

  /**
   * If the file is a folder, returns the {@link FileInfo} of all the direct entries in it.
   * Otherwise returns the FileInfo for the file.
   *
   * @param file the handler for the file
   * @return a list of FileInfos representing the files which are children of the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if the master cannot retrieve the file status for some other reason
   */
  List<FileInfo> listStatus(TachyonFile file) throws IOException, FileDoesNotExistException;

  /**
   * Creates a folder. If the parent folders do not exist, they will be created automatically.
   *
   * @param path the handler for the file
   * @return true if the folder is created successfully or already existing, false otherwise.
   * @throws InvalidPathException if the provided path is invalid
   * @throws FileAlreadyExistException if there is already a file at the given path
   * @throws IOException if the master cannot create the folder under the specified path
   */
  boolean mkdirs(TachyonURI path) throws IOException, InvalidPathException,
      FileAlreadyExistException;

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return a TachyonFile which acts as a file handler for the path
   * @throws InvalidPathException if the provided path is invalid
   * @throws IOException if the path does not exist in Tachyon space
   */
  TachyonFile open(TachyonURI path) throws IOException, InvalidPathException;

  /**
   * Renames an existing file in Tachyon space to another path in Tachyon space.
   *
   * @param src The file handler for the source file
   * @param dst The path of the destination file, this path should not exist
   * @return true if successful, false otherwise
   * @throws FileDoesNotExistException if the source file does not exist
   * @throws IOException if the destination already exists or is invalid
   */
  boolean rename(TachyonFile src, TachyonURI dst) throws IOException, FileDoesNotExistException;


  /**
   * Sets the pin status of a file. A pinned file will never be evicted for any reason. The pin
   * status is propagated asynchronously from this method call on the worker heartbeats.
   *
   * @param file the file handler for the file to pin
   * @param pinned true to pin the file, false to unpin it
   * @throws IOException if an error occurs during the pin operation
   */
  void setPin(TachyonFile file, boolean pinned) throws IOException, FileDoesNotExistException;
}
