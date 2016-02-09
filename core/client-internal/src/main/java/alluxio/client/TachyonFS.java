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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.DeleteOptions;

import java.io.IOException;

/**
 * @deprecated {@see FileSystem} for the supported API.
 * Represents an Alluxio file system, legacy API.
 */
@Deprecated
public final class TachyonFS {
  private final FileSystem mFileSystem;

  /**
   * @param path path corresponding to the file system
   * @param conf configuration to use
   * @return an instance of {@link TachyonFS}
   */
  public static TachyonFS get(AlluxioURI path, Configuration conf) {
    return new TachyonFS();
  }

  private TachyonFS() {
    mFileSystem = FileSystem.Factory.get();
  }

  /**
   * Closes the TachyonFS.
   */
  public void close() {
    // Do nothing
  }

  /**
   * Creates a file in Alluxio. Creates are done through {@link TachyonFile#getOutStream} so this
   * method is a no-op to comply with past APIs.
   * @param path the path to create
   * @return the file id, in this case always -1
   */
  public long createFile(AlluxioURI path) {
    // Do nothing
    return -1;
  }

  /**
   * Deletes a file in Alluxio.
   * @param path the path to delete
   * @return true if the file was deleted
   * @throws IOException if the file was unable to be deleted
   */
  public boolean delete(AlluxioURI path) throws IOException {
    try {
      mFileSystem.delete(path);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Deletes a file in Alluxio.
   * @param path the path to delete
   * @param recursive if true will delete all children of the path as well
   * @return true if the file was deleted
   * @throws IOException if the file was unable to be deleted
   */
  public boolean delete(AlluxioURI path, boolean recursive) throws IOException {
    try {
      mFileSystem.delete(path, DeleteOptions.defaults().setRecursive(recursive));
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @param path the path to check for existence
   * @return true if the path exists, false otherwise
   * @throws IOException if an error occurs when interacting with a non Alluxio component
   */
  public boolean exist(AlluxioURI path) throws IOException {
    try {
      return mFileSystem.exists(path);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @param path the path specifying the file to get
   * @return the {@link TachyonFile} object referencing the resource at the path
   */
  public TachyonFile getFile(AlluxioURI path) {
    return new TachyonFile(path, mFileSystem);
  }

  /**
   * @param path the path to make a directory at
   * @return true if successful
   * @throws IOException if the directory could not be created
   */
  public boolean mkdir(AlluxioURI path) throws IOException {
    try {
      mFileSystem.createDirectory(path);
      return true;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
