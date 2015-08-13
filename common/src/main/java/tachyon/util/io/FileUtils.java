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

package tachyon.util.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;

import tachyon.TachyonURI;
import tachyon.thrift.InvalidPathException;

/**
 * Provides utility methods for working with files and directories.
 *
 * By convention, methods take file path strings as parameters.
 */
public class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger("");

  /**
   * Change local file's permission.
   *
   * @param filePath that will change permission
   * @param perms the permission, e.g. "775"
   * @throws IOException when fails to change permission
   */
  public static void changeLocalFilePermission(String filePath, String perms) throws IOException {
    // TODO switch to java's Files.setPosixFilePermissions() if java 6 support is dropped
    List<String> commands = new ArrayList<String>();
    commands.add("/bin/chmod");
    commands.add(perms);
    File file = new File(filePath);
    commands.add(file.getAbsolutePath());

    try {
      ProcessBuilder builder = new ProcessBuilder(commands);
      Process process = builder.start();

      process.waitFor();

      redirectIO(process);

      if (process.exitValue() != 0) {
        throw new IOException("Can not change the file " + file.getAbsolutePath()
            + " 's permission to be " + perms);
      }
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    }
  }

  /**
   * Blocking operation that copies the processes stdout/stderr to this JVM's stdout/stderr.
   */
  private static void redirectIO(final Process process) throws IOException {
    // Because chmod doesn't have a lot of error or output messages, its safe to process the output
    // after the process is done. As of java 7, you can have the process redirect to System.out
    // and System.err without forking a process.
    // TODO when java 6 support is dropped, switch to
    // http://docs.oracle.com/javase/7/docs/api/java/lang/ProcessBuilder.html#inheritIO()
    Closer closer = Closer.create();
    try {
      ByteStreams.copy(closer.register(process.getInputStream()), System.out);
      ByteStreams.copy(closer.register(process.getErrorStream()), System.err);
    } catch (Throwable e) {
      throw closer.rethrow(e);
    } finally {
      closer.close();
    }
  }

  /**
   * Change local file's permission to be 777.
   *
   * @param filePath that will change permission
   * @throws IOException when fails to change file's permission to 777
   */
  public static void changeLocalFileToFullPermission(String filePath) throws IOException {
    changeLocalFilePermission(filePath, "777");
  }

  /**
   * If the sticky bit of is set on the directory 'dir', the 'dir' can only 
   * be deleted or renamed by the owner 
   *
   * @param dir absolute dir path
   * @throws IOException when fails to set sticky bit
   */
  public static void setLocalDirStickyBit(String dir) {
    try {
      // sticky bit is not implemented in PosixFilePermission
      if (dir.startsWith(TachyonURI.SEPARATOR)) {
        Runtime.getRuntime().exec("chmod o+t " + dir);
      }
    } catch (IOException e) {
      LOG.info("Can not set the sticky bit of the direcotry : " + dir);
    }
  }

  /**
   * Creates the local block path and all the parent directories. Also, sets the appropriate
   * permissions.
   *
   * @param path The path of the block.
   * @throws IOException when fails to create block path and parent directories with appropriate
   *         permissions.
   */
  public static void createBlockPath(String path) throws IOException {
    File localFolder;
    try {
      localFolder = new File(PathUtils.getParent(path));
    } catch (InvalidPathException e) {
      throw new IOException(e);
    }

    if (!localFolder.exists()) {
      if (localFolder.mkdirs()) {
        changeLocalFileToFullPermission(localFolder.getAbsolutePath());
        LOG.info("Folder {} was created!", localFolder);
      } else {
        throw new IOException("Failed to create folder " + localFolder);
      }
    }
  }

  /**
   * Move file from one place to another, can across storage devices (e.g., from memory to SSD) when
   * {@link File#renameTo} may not work.
   *
   * Current implementation uses {@link com.google.common.io.Files#move(File, File);}, may change if
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
   * Delete the file or directory.
   *
   * Current implementation uses {@link java.io.File#delete()}, may change if
   * there is a better solution.
   *
   * @param path pathname string of file or directory
   * @throws IOException when fails to delete
   */
  public static void delete(String path) throws IOException {
    File file = new File(path);
    boolean deletionSucceeded = file.delete();
    if (deletionSucceeded == false) {
      throw new IOException("Failed to delete " + path);
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
}
