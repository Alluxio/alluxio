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

package tachyon.client.next.file;

import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.thrift.FileInfo;

/**
 * User facing interface for the Tachyon File System client APIs. File refers to any type of inode,
 * including folders. Clients should provide their own interface for reading/writing files.
 */
interface TachyonFSCore {

  /**
   * Deletes a file. If the file is a folder, its contents will be deleted recursively.
   *
   * @param file the handler of the file to delete.
   * @throws IOException if the master is unable to delete the file
   */
  void delete(TachyonFile file) throws IOException;

  /**
   * Removes the file from Tachyon Storage. The underlying under storage system file will not be
   * removed. If the file is a folder, its contents will be freed recursively.
   *
   * @param file the handler for the file
   * @throws IOException if the master is unable to free the file
   */
  void free(TachyonFile file) throws IOException;

  /**
   * Gets the FileInfo object that represents the Tachyon file
   *
   * @param file the handler for the file.
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if the master is unable to obtain the file's metadata
   */
  FileInfo getInfo(TachyonFile file) throws IOException;

  /**
   * If the file is a folder, return the FileInfo of all the direct entries in it. Otherwise return
   * the FileInfo for the file.
   *
   * @param file the handler for the file
   * @return A list of FileInfo, null if the file or folder does not exist.
   * @throws IOException if the master is unable to obtain the metadata
   */
  List<FileInfo> listStatus(TachyonFile file) throws IOException;

  /**
   * Creates a folder. If the parent folders do not exist, they will be created automatically.
   *
   * @param file the handler for the file
   * @return true if the folder is created successfully or already existing, false otherwise.
   * @throws IOException if the master cannot create the folder under the specified path
   */
  boolean mkdirs(TachyonFile file) throws IOException;

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as a handler for the file.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return the TachyonFile representation of the file which can be used to reference the file
   */
  TachyonFile open(TachyonURI path) throws IOException;

  /**
   * Renames a file to another path.
   *
   * @param src the handler for the source file.
   * @param dst The path of the destination file, this path should not exist
   * @return true if renames successfully, false otherwise.
   * @throws IOException if the master cannot rename the src to dst
   */
  boolean rename(TachyonFile src, TachyonURI dst) throws IOException;
}
