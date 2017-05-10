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

package alluxio.underfs;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import alluxio.underfs.options.MkdirsOptions;

/**
 * Extend {@link UnderFileSystem} with directory operations.
 */
@ThreadSafe
public interface DirectoryUnderFileSystem extends UnderFileSystem {

  /**
   * Gets the directory status.
   *
   * @param path the file name
   * @return the directory status
   */
  UfsDirectoryStatus getDirectoryStatus(String path) throws IOException;

  /**
   * Checks if a directory exists in under file system.
   *
   * @param path the absolute directory path
   * @return true if the path exists and is a directory, false otherwise
   */
  boolean isDirectory(String path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname. If the folder already exists, the method
   * returns false. The method creates any necessary but nonexistent parent directories.
   *
   * @param path the folder to create
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   */
  boolean mkdirs(String path) throws IOException;

  /**
   * Creates the directory named by this abstract pathname, with specified
   * {@link MkdirsOptions}. If the folder already exists, the method returns false.
   *
   * @param path the folder to create
   * @param options the options for mkdirs
   * @return {@code true} if and only if the directory was created; {@code false} otherwise
   */
  boolean mkdirs(String path, MkdirsOptions options) throws IOException;
}
