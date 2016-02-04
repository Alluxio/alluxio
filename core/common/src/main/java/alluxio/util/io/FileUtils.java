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

package alluxio.util.io;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;
import alluxio.util.ShellUtils;

/**
 * Provides utility methods for working with files and directories.
 *
 * By convention, methods take file path strings as parameters.
 */
@ThreadSafe
public final class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Changes local file's permission.
   *
   * @param filePath that will change permission
   * @param perms the permission, e.g. "775"
   * @throws IOException when fails to change permission
   */
  public static void changeLocalFilePermission(String filePath, String perms) throws IOException {
    // TODO(cc): Switch to java's Files.setPosixFilePermissions() when Java 6 support is dropped.
    ShellUtils.execCommand(ShellUtils.getSetPermissionCommand(perms, filePath));
  }

  /**
   * Changes local file's permission to be 777.
   *
   * @param filePath that will change permission
   * @throws IOException when fails to change file's permission to 777
   */
  public static void changeLocalFileToFullPermission(String filePath) throws IOException {
    changeLocalFilePermission(filePath, "777");
  }

  /**
   * Sticky bit can be set primarily on directories in UNIX / Linux.
   *
   * If the sticky bit of is enabled on a directory, only the owner and the root user can delete /
   * rename the files or directories within that directory. No one else can delete other users data
   * in this directory(Where sticky bit is set).
   *
   * This is a security measure to avoid deletion of folders and their content (sub-folders and
   * files), though other users have full permissions.
   *
   * Setting the sticky bit of a file is a no-op.
   *
   * @param dir absolute dir path to set the sticky bit
   */
  public static void setLocalDirStickyBit(String dir) {
    try {
      // Support for sticky bit is platform specific. Check if the path starts with "/" and if so,
      // assume that the host supports the chmod command.
      if (dir.startsWith(AlluxioURI.SEPARATOR)) {
        Runtime.getRuntime().exec("chmod +t " + dir);
      }
    } catch (IOException e) {
      LOG.info("Can not set the sticky bit of the directory: {}", dir, e);
    }
  }

  /**
   * Creates the local block path and all the parent directories. Also, sets the appropriate
   * permissions.
   *
   * @param path the path of the block
   * @throws IOException when fails to create block path and parent directories with appropriate
   *         permissions.
   */
  public static void createBlockPath(String path) throws IOException {
    try {
      createStorageDirPath(PathUtils.getParent(path));
    } catch (InvalidPathException e) {
      throw new IOException("Failed to create block path, get parent path of " + path + "failed",
          e);
    } catch (IOException e) {
      throw new IOException("Failed to create block path " + path, e);
    }
  }

  /**
   * Moves file from one place to another, can across storage devices (e.g., from memory to SSD)
   * when {@link File#renameTo} may not work.
   *
   * Current implementation uses {@link com.google.common.io.Files#move(File, File)}, may change if
   * there is a better solution.
   *
   * @param srcPath pathname string of source file
   * @param dstPath pathname string of destination file
   * @throws IOException when fails to move
   */
  public static void move(String srcPath, String dstPath) throws IOException {
    Files.move(new File(srcPath), new File(dstPath));
  }

  /**
   * Deletes the file or directory.
   *
   * Current implementation uses {@link java.io.File#delete()}, may change if there is a better
   * solution.
   *
   * @param path pathname string of file or directory
   * @throws IOException when fails to delete
   */
  public static void delete(String path) throws IOException {
    File file = new File(path);
    if (!file.delete()) {
      throw new IOException("Failed to delete " + path);
    }
  }

  /**
   * Creates the storage directory path, including any necessary but nonexistent parent directories.
   * If the directory already exists, do nothing.
   *
   * Also, appropriate directory permissions (777 + StickyBit, namely "drwxrwxrwt") are set.
   *
   * @param path storage directory path to create
   * @throws IOException when fails to create storage directory path
   */
  public static void createStorageDirPath(String path) throws IOException {
    File dir = new File(path);
    String absolutePath = dir.getAbsolutePath();
    if (!dir.exists()) {
      if (dir.mkdirs()) {
        changeLocalFileToFullPermission(absolutePath);
        setLocalDirStickyBit(absolutePath);
        LOG.info("Folder {} was created!", path);
      } else {
        throw new IOException("Failed to create folder " + path);
      }
    }
  }

  /**
   * Creates an empty file and its intermediate directories if necessary.
   *
   * @param filePath pathname string of the file to create
   * @throws IOException if an I/O error occurred or file already exists
   */
  public static void createFile(String filePath) throws IOException {
    File file = new File(filePath);
    Files.createParentDirs(file);
    if (!file.createNewFile()) {
      throw new IOException("File already exists " + filePath);
    }
  }

  /**
   * Creates an empty directory and its intermediate directories if necessary.
   *
   * @param path path of the directory to create
   * @throws IOException if an I/O error occurred or directory already exists
   */
  public static void createDir(String path) throws IOException {
    new File(path).mkdirs();
  }

  /**
   * Checks if a path exists.
   *
   * @param path the given path
   * @return true if path exists, false otherwise
   */
  public static boolean exists(String path) {
    return new File(path).exists();
  }

  private FileUtils() {} // prevent instantiation
}
