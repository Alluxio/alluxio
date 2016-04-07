/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.util.io;

import alluxio.AlluxioURI;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.util.OSUtils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities related to both Alluxio paths like {@link AlluxioURI} and local file paths.
 */
@ThreadSafe
public final class PathUtils {

  /**
   * Checks and normalizes the given path.
   *
   * @param path The path to clean up
   * @return a normalized version of the path, with single separators between path components and
   *         dot components resolved
   * @throws InvalidPathException if the path is invalid
   */
  public static String cleanPath(String path) throws InvalidPathException {
    validatePath(path);
    return FilenameUtils.separatorsToUnix(FilenameUtils.normalizeNoEndSeparator(path));
  }

  /**
   * Joins each element in paths in order, separated by {@link AlluxioURI#SEPARATOR}.
   * <p>
   * For example,
   *
   * <pre>
   * {@code
   * concatPath("/myroot/", "dir", 1L, "filename").equals("/myroot/dir/1/filename");
   * concatPath("alluxio://myroot", "dir", "filename").equals("alluxio://myroot/dir/filename");
   * concatPath("myroot/", "/dir/", "filename").equals("myroot/dir/filename");
   * concatPath("/", "dir", "filename").equals("/dir/filename");
   * }
   * </pre>
   *
   * Note that empty element in base or paths is ignored.
   *
   * @param base base path
   * @param paths paths to concatenate
   * @return joined path
   * @throws IllegalArgumentException if base or paths is null
   */
  public static String concatPath(Object base, Object... paths) throws IllegalArgumentException {
    Preconditions.checkArgument(base != null, "Failed to concatPath: base is null");
    Preconditions.checkArgument(paths != null, "Failed to concatPath: a null set of paths");
    List<String> trimmedPathList = new ArrayList<String>();
    String trimmedBase =
        CharMatcher.is(AlluxioURI.SEPARATOR.charAt(0)).trimTrailingFrom(base.toString().trim());
    trimmedPathList.add(trimmedBase);
    for (Object path : paths) {
      if (path == null) {
        continue;
      }
      String trimmedPath =
          CharMatcher.is(AlluxioURI.SEPARATOR.charAt(0)).trimFrom(path.toString().trim());
      if (!trimmedPath.isEmpty()) {
        trimmedPathList.add(trimmedPath);
      }
    }
    if (trimmedPathList.size() == 1 && trimmedBase.isEmpty()) {
      // base must be "[/]+"
      return AlluxioURI.SEPARATOR;
    }
    return Joiner.on(AlluxioURI.SEPARATOR).join(trimmedPathList);

  }

  /**
   * Gets the parent of the file at a path.
   *
   * @param path The path
   * @return the parent path of the file; this is "/" if the given path is the root
   * @throws InvalidPathException if the path is invalid
   */
  public static String getParent(String path) throws InvalidPathException {
    String cleanedPath = cleanPath(path);
    String name = FilenameUtils.getName(cleanedPath);
    String parent = cleanedPath.substring(0, cleanedPath.length() - name.length() - 1);
    if (parent.isEmpty()) {
      // The parent is the root path
      return AlluxioURI.SEPARATOR;
    }
    return parent;
  }

  /**
   * Gets the path components of the given path. The first component will be an empty string.
   *
   * "/a/b/c" => {"", "a", "b", "c"}
   * "/" => {""}
   *
   * @param path The path to split
   * @return the path split into components
   * @throws InvalidPathException if the path is invalid
   */
  public static String[] getPathComponents(String path) throws InvalidPathException {
    path = cleanPath(path);
    if (isRoot(path)) {
      return new String[]{""};
    }
    return path.split(AlluxioURI.SEPARATOR);
  }

  /**
   * Checks whether the given path contains the given prefix. The comparison happens at a component
   * granularity; for example, {@code hasPrefix(/dir/file, /dir)} should evaluate to true, while
   * {@code hasPrefix(/dir/file, /d)} should evaluate to false.
   *
   * @param path a path
   * @param prefix a prefix
   * @return whether the given path has the given prefix
   * @throws InvalidPathException when the path or prefix is invalid
   */
  public static boolean hasPrefix(String path, String prefix) throws InvalidPathException {
    String[] pathComponents = getPathComponents(path);
    String[] prefixComponents = getPathComponents(prefix);
    if (pathComponents.length < prefixComponents.length) {
      return false;
    }
    for (int i = 0; i < prefixComponents.length; i++) {
      if (!pathComponents[i].equals(prefixComponents[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks if the given path is the root.
   *
   * @param path The path to check
   * @return true if the path is the root
   * @throws InvalidPathException if the path is invalid
   */
  public static boolean isRoot(String path) throws InvalidPathException {
    return AlluxioURI.SEPARATOR.equals(cleanPath(path));
  }

  /**
   * Checks if the given path is properly formed.
   *
   * @param path The path to check
   * @throws InvalidPathException If the path is not properly formed
   */
  public static void validatePath(String path) throws InvalidPathException {
    boolean invalid = (path == null || path.isEmpty() || path.contains(" "));
    if (!OSUtils.isWindows()) {
      invalid = (invalid || !path.startsWith(AlluxioURI.SEPARATOR));
    }

    if (invalid) {
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID.getMessage(path));
    }
  }

  /**
   * Generates a deterministic temporary file name for the a path and a file id and a nonce.
   *
   * @param nonce a nonce token
   * @param path a file path
   * @return a deterministic temporary file name
   */
  public static String temporaryFileName(long nonce, String path) {
    return path + ".alluxio." + String.format("0x%16X", nonce) + ".tmp";
  }

  /**
   * Creates a unique path based off the caller.
   *
   * @return unique path based off the caller
   */
  public static String uniqPath() {
    StackTraceElement caller = new Throwable().getStackTrace()[1];
    long time = System.nanoTime();
    return "/" + caller.getClassName() + "/" + caller.getMethodName() + "/" + time;
  }

  /**
   * Adds a trailing separator if it does not exists in path.
   *
   * @param path the file name
   * @param separator trailing separator to add
   * @return updated path with trailing separator
   */
  public static String normalizePath(String path, String separator) {
    return path.endsWith(separator) ? path : path + separator;
  }

  private PathUtils() {} // prevent instantiation
}
