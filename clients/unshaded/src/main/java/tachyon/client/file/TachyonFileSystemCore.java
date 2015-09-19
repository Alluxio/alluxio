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
import tachyon.thrift.BlockInfoException;
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
   * Creates a file with the provided block size as the standard block size of the file. If the
   * file's parent directories do not exist, they will be created if the recursive flag is set.
   *
   * @param path the path of the file to create in Tachyon space
   * @param blockSize the block size in bytes, must be greater than 0
   * @param recursive whether or not to create parent directories if required
   * @return the file id that identifies the newly created file
   * @throws BlockInfoException if the block size is less than or equal to 0
   * @throws FileAlreadyExistException if the path already exists as a file in Tachyon
   * @throws InvalidPathException if the path is not a valid Tachyon path
   * @throws IOException if the master is unable to create the file
   */
  long create(TachyonURI path, long blockSize, boolean recursive) throws BlockInfoException,
      FileAlreadyExistException, InvalidPathException, IOException;

  /**
   * Deletes a file. If the file is a folder, its contents will be deleted recursively if the
   * flag is set.
   *
   * @param file the handler of the file to delete.
   * @param recursive whether or not to delete all contents in a non empty folder
   * @throws FileDoesNotExistException if the given file does not exist
   * @throws IOException if the master is unable to delete the file
   */
  void delete(TachyonFile file, boolean recursive) throws FileDoesNotExistException, IOException;

  /**
   * Removes the file from Tachyon Storage. The underlying under storage system file will not be
   * removed. If the file is a folder, its contents will be freed recursively if the flag is set.
   *
   * @param file the handler for the file
   * @param recursive whether or not to free all contents in a non empty folder
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if the master is unable to free the file for some other reason
   */
  void free(TachyonFile file, boolean recursive) throws FileDoesNotExistException, IOException;

  /**
   * Gets the {@link FileInfo} object that represents the metadata of a Tachyon file.
   *
   * @param file the handler for the file.
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if the master cannot retrieve the file's metadata for some other reason
   */
  FileInfo getInfo(TachyonFile file) throws IOException;

  /**
   * If the file is a folder, returns the {@link FileInfo} of all the direct entries in it.
   * Otherwise returns the {@link FileInfo} for the file.
   *
   * @param file the handler for the file
   * @return a list of FileInfos representing the files which are children of the given file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if the master cannot retrieve the file status for some other reason
   */
  List<FileInfo> listStatus(TachyonFile file) throws FileDoesNotExistException, IOException;

  /**
   * Adds metadata about a file in the under storage system to Tachyon. Only metadata will be
   * updated and no data will be transferred.
   *
   * @param path the path to create the file in Tachyon
   * @param recursive if true, the parent directories to the file in Tachyon will be created
   * @return the file id of the resulting file in Tachyon
   * @throws FileDoesNotExistException if there is no file at the given path
   * @throws IOException if the Tachyon path is invalid or the ufsPath does not exist
   */
  long loadFileInfoFromUfs(TachyonURI path, boolean recursive)
      throws FileDoesNotExistException, IOException;

  /**
   * Creates a folder. If the parent folders do not exist, they will be created automatically if
   * the recursive flag is set.
   *
   * @param path the handler for the file
   * @param recursive whether or not to create the parent folders that do not exist
   * @return true if the folder is created successfully or already existing, false otherwise.
   * @throws FileAlreadyExistException if there is already a file at the given path
   * @throws InvalidPathException if the provided path is invalid
   * @throws IOException if the master cannot create the folder under the specified path
   */
  boolean mkdirs(TachyonURI path, boolean recursive) throws FileAlreadyExistException,
      InvalidPathException, IOException;

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return a TachyonFile which acts as a file handler for the path
   * @throws InvalidPathException if the provided path is invalid
   * @throws IOException if the path does not exist in Tachyon space
   */
  TachyonFile open(TachyonURI path) throws InvalidPathException, IOException;

  /**
   * Renames an existing file in Tachyon space to another path in Tachyon space.
   *
   * @param src The file handler for the source file
   * @param dst The path of the destination file, this path should not exist
   * @return true if successful, false otherwise
   * @throws FileDoesNotExistException if the source file does not exist
   * @throws IOException if the destination already exists or is invalid
   */
  boolean rename(TachyonFile src, TachyonURI dst) throws FileDoesNotExistException, IOException;

  /**
   * Sets the pin status of a file. A pinned file will never be evicted for any reason.
   *
   * @param file the file handler for the file to pin
   * @param pinned true to pin the file, false to unpin it
   * @throws FileDoesNotExistException if the file to be pinned does not exist
   * @throws IOException if an error occurs during the pin operation
   */
  void setPin(TachyonFile file, boolean pinned) throws FileDoesNotExistException, IOException;
}
