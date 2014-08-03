/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon;

import java.io.IOException;

/**
 * Utility functions for working with {@link tachyon.UnderFileSystem}.
 */
public final class UnderFileSystemsUtils {
  private UnderFileSystemsUtils() {}

  /**
   * Deletes the directory at the given path. If delete is unsuccessful, then this operation will
   * throw a {@link java.io.IOException}.
   */
  public static void deleteDir(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (ufs.exists(path) && !ufs.delete(path, true)) {
      throw new IOException("Folder " + path + " already exists but can not be deleted.");
    }
  }

  /**
   * Attempts to create the directory if it does not already exist. If unable to create the
   * directory, then a {@link java.io.IOException} is thrown.
   */
  public static void mkdirIfNotExists(final String path) throws IOException {
    UnderFileSystem ufs = UnderFileSystem.get(path);

    if (!ufs.exists(path)) {
      if (!ufs.mkdirs(path, true)) {
        throw new IOException("Failed to make folder: " + path);
      }
    }
  }
}
