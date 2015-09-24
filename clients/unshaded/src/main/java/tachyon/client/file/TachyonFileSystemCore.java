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
import tachyon.client.options.CreateOptions;
import tachyon.client.options.DeleteOptions;
import tachyon.client.options.FreeOptions;
import tachyon.client.options.GetInfoOptions;
import tachyon.client.options.ListStatusOptions;
import tachyon.client.options.LoadMetadataOptions;
import tachyon.client.options.MkdirOptions;
import tachyon.client.options.OpenOptions;
import tachyon.client.options.RenameOptions;
import tachyon.client.options.SetStateOptions;
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
   * @throws TachyonException if a Tachyon exception occurs
   */
  long create(TachyonURI path, CreateOptions options) throws IOException, TachyonException;

  /**
   * Deletes a file or a directory.
   *
   * @param file the handler of the file to delete
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  void delete(TachyonFile file, DeleteOptions options) throws IOException, TachyonException;

  /**
   * Removes the file from Tachyon, but not from UFS in case it exists there.
   *
   * @param file the handler for the file
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  void free(TachyonFile file, FreeOptions options) throws IOException, TachyonException;

  /**
   * Gets the {@link FileInfo} object that represents the metadata of a Tachyon file.
   *
   * @param file the handler for the file.
   * @param options method options
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
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
   * @throws TachyonException if a Tachyon exception occurs
   */
  List<FileInfo> listStatus(TachyonFile file, ListStatusOptions options) throws IOException,
      TachyonException;

  /**
   * Loads metadata about a file in UFS to Tachyon. No data will be transferred.
   *
   * @param path the path to create the file in Tachyon
   * @param ufsPath the UFS path of the file that will back the Tachyon file
   * @param options method options
   * @return the file id of the resulting file in Tachyon
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  long loadMetadata(TachyonURI path, TachyonURI ufsPath, LoadMetadataOptions options)
      throws IOException, TachyonException;

  /**
   * Creates a directory.
   *
   * @param path the handler for the file
   * @param options method options
   * @return true if the directory is created successfully or already existing, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  boolean mkdir(TachyonURI path, MkdirOptions options) throws IOException, TachyonException;

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @param options method options
   * @return a TachyonFile which acts as a file handler for the path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  TachyonFile open(TachyonURI path, OpenOptions options) throws IOException, TachyonException;

  /**
   * Renames an existing Tachyon file to another Tachyon path in Tachyon.
   *
   * @param src the file handler for the source file
   * @param dst the path of the destination file, this path should not exist
   * @param options method options
   * @return true if successful, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  boolean rename(TachyonFile src, TachyonURI dst, RenameOptions options) throws IOException,
      TachyonException;

  /**
   * Sets the state of a file.
   *
   * @param file the file handler for the file to pin
   * @param options method options
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  void setState(TachyonFile file, SetStateOptions options) throws IOException, TachyonException;
}
