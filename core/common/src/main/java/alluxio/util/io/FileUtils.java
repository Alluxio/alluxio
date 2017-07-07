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

package alluxio.util.io;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Provides utility methods for working with files and directories.
 *
 * By convention, methods take file path strings as parameters.
 */
@ThreadSafe
public final class FileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(FileUtils.class);

  /**
   * Changes the local file's group.
   *
   * @param path that will change owner
   * @param group the new group
   */
  public static void changeLocalFileGroup(String path, String group) throws IOException {
    UserPrincipalLookupService lookupService =
        FileSystems.getDefault().getUserPrincipalLookupService();
    PosixFileAttributeView view =
        Files.getFileAttributeView(Paths.get(path), PosixFileAttributeView.class,
            LinkOption.NOFOLLOW_LINKS);
    GroupPrincipal groupPrincipal = lookupService.lookupPrincipalByGroupName(group);
    view.setGroup(groupPrincipal);
  }

  /**
   * Changes local file's permission.
   *
   * @param filePath that will change permission
   * @param perms the permission, e.g. "rwxr--r--"
   */
  public static void changeLocalFilePermission(String filePath, String perms) throws IOException {
    Files.setPosixFilePermissions(Paths.get(filePath), PosixFilePermissions.fromString(perms));
  }

  /**
   * Changes local file's permission to be "rwxrwxrwx".
   *
   * @param filePath that will change permission
   */
  public static void changeLocalFileToFullPermission(String filePath) throws IOException {
    changeLocalFilePermission(filePath, "rwxrwxrwx");
  }

  /**
   * Gets local file's owner.
   *
   * @param filePath the file path
   * @return the owner of the local file
   */
  public static String getLocalFileOwner(String filePath) throws IOException {
    PosixFileAttributes attr =
        Files.readAttributes(Paths.get(filePath), PosixFileAttributes.class);
    return attr.owner().getName();
  }

  /**
   * Gets local file's group.
   *
   * @param filePath the file path
   * @return the group of the local file
   */
  public static String getLocalFileGroup(String filePath) throws IOException {
    PosixFileAttributes attr =
        Files.readAttributes(Paths.get(filePath), PosixFileAttributes.class);
    return attr.group().getName();
  }

  /**
   * Gets local file's permission mode.
   *
   * @param filePath the file path
   * @return the file mode in short, e.g. 0777
   */
  public static short getLocalFileMode(String filePath) throws IOException {
    Set<PosixFilePermission> permission =
        Files.readAttributes(Paths.get(filePath), PosixFileAttributes.class).permissions();
    return translatePosixPermissionToMode(permission);
  }

  /**
   * Translate posix file permissions to short mode.
   *
   * @param permission posix file permission
   * @return mode for file
   */
  public static short translatePosixPermissionToMode(Set<PosixFilePermission> permission) {
    int mode = 0;
    for (PosixFilePermission action : PosixFilePermission.values()) {
      mode = mode << 1;
      mode += permission.contains(action) ? 1 : 0;
    }
    return (short) mode;
  }

  /**
   * Changes the local file's user.
   *
   * @param path that will change owner
   * @param user the new user
   */
  public static void changeLocalFileUser(String path, String user) throws IOException {
    UserPrincipalLookupService lookupService =
        FileSystems.getDefault().getUserPrincipalLookupService();
    PosixFileAttributeView view =
        Files.getFileAttributeView(Paths.get(path), PosixFileAttributeView.class,
            LinkOption.NOFOLLOW_LINKS);
    UserPrincipal userPrincipal = lookupService.lookupPrincipalByName(user);
    view.setOwner(userPrincipal);
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
        // TODO(peis): This is very slow. Consider removing this.
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
   * @param srcPath pathname string of source file
   * @param dstPath pathname string of destination file
   */
  public static void move(String srcPath, String dstPath) throws IOException {
    Files.move(Paths.get(srcPath), Paths.get(dstPath), StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Deletes the file or directory.
   *
   * @param path pathname string of file or directory
   */
  public static void delete(String path) throws IOException {
    if (!Files.deleteIfExists(Paths.get(path))) {
      throw new IOException("Failed to delete " + path);
    }
  }

  /**
   * Deletes a file or a directory, recursively if it is a directory.
   *
   * @param path pathname to be deleted
   */
  public static void deletePathRecursively(String path) throws IOException {
    Path root = Paths.get(path);
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException e) throws IOException {
        if (e == null) {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        } else {
          throw e;
        }
      }
    });
  }

  /**
   * Creates the storage directory path, including any necessary but nonexistent parent directories.
   * If the directory already exists, do nothing.
   *
   * Also, appropriate directory permissions (777 + StickyBit, namely "drwxrwxrwt") are set.
   *
   * @param path storage directory path to create
   */
  public static void createStorageDirPath(String path) throws IOException {
    if (Files.exists(Paths.get(path))) {
      return;
    }
    Path storagePath;
    try {
      storagePath = Files.createDirectories(Paths.get(path));
    } catch (UnsupportedOperationException | SecurityException | IOException e) {
      throw new IOException("Failed to create folder " + path, e);
    }
    String absolutePath = storagePath.toAbsolutePath().toString();
    changeLocalFileToFullPermission(absolutePath);
    setLocalDirStickyBit(absolutePath);
    LOG.info("Folder {} was created!", path);
  }

  /**
   * Creates an empty file and its intermediate directories if necessary.
   *
   * @param filePath pathname string of the file to create
   */
  public static void createFile(String filePath) throws IOException {
    Path storagePath = Paths.get(filePath);
    Files.createDirectories(storagePath.getParent());
    Files.createFile(storagePath);
  }

  /**
   * Creates an empty directory and its intermediate directories if necessary.
   *
   * @param path path of the directory to create
   */
  public static void createDir(String path) throws IOException {
    Files.createDirectories(Paths.get(path));
  }

  /**
   * Checks if a path exists.
   *
   * @param path the given path
   * @return true if path exists, false otherwise
   */
  public static boolean exists(String path) {
    return Files.exists(Paths.get(path));
  }

  private FileUtils() {} // prevent instantiation
}
