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

package alluxio.util;

import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.DeleteOptions;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with {@link alluxio.underfs.UnderFileSystem}.
 *
 */
@ThreadSafe
public final class UnderFileSystemUtils {

  /**
   * Deletes the directory at the given path if it exists.
   *
   * @param path path to the directory
   * @throws IOException if the directory cannot be deleted
   */
  public static void deleteDirIfExists(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.isDirectory(path)
        && !ufs.deleteDirectory(path, new DeleteOptions().setRecursive(true))) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  /**
   * Attempts to create the directory if it does not already exist.
   *
   * @param path path to the directory
   * @throws IOException if the directory cannot be created
   */
  public static void mkdirIfNotExists(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (!ufs.isDirectory(path)) {
      if (!ufs.mkdirs(path, true)) {
        throw new IOException("Failed to make folder: " + path);
      }
    }
  }

  /**
   * Creates an empty file.
   *
   * @param path path to the file
   * @throws IOException if the file cannot be created
   */
  public static void touch(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    OutputStream os = ufs.create(path);
    os.close();
  }

  /**
   * Deletes the specified path from the specified under file system if it is a file and exists.
   *
   * @param path the path to delete
   */
  public static void deleteFileIfExists(final String path) {
    UnderFileSystem ufs = UnderFileSystem.get(path);
    try {
      if (ufs.isFile(path)) {
        ufs.deleteFile(path);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private UnderFileSystemUtils() {} // prevent instantiation
}
