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

package tachyon.util;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.ThreadSafe;

import tachyon.conf.TachyonConf;
import tachyon.underfs.UnderFileSystem;

/**
 * Utility functions for working with {@link tachyon.underfs.UnderFileSystem}.
 *
 */
@ThreadSafe
public final class UnderFileSystemUtils {

  /**
   * Deletes the directory at the given path.
   *
   * @param path path to the directory
   * @param tachyonConf Tachyon configuration
   * @throws IOException if the directory cannot be deleted
   */
  public static void deleteDir(final String path, TachyonConf tachyonConf) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, tachyonConf);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  /**
   * Attempts to create the directory if it does not already exist.
   *
   * @param path path to the directory
   * @param tachyonConf Tachyon configuration
   * @throws IOException if the directory cannot be created
   */
  public static void mkdirIfNotExists(final String path, TachyonConf tachyonConf)
      throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, tachyonConf);

    if (!ufs.exists(path)) {
      if (!ufs.mkdirs(path, true)) {
        throw new IOException("Failed to make folder: " + path);
      }
    }
  }

  /**
   * Creates an empty file.
   *
   * @param path path to the file
   * @param tachyonConf Tachyon Configuration
   * @throws IOException if the file cannot be created
   */
  public static void touch(final String path, TachyonConf tachyonConf) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path, tachyonConf);
    OutputStream os = ufs.create(path);
    os.close();
  }

  private UnderFileSystemUtils() {} // prevent instantiation
}
