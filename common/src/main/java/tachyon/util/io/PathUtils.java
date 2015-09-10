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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.thrift.InvalidPathException;

/**
 * Utilities related to both Tachyon paths like {@link tachyon.TachyonURI} and local file paths.
 */
public class PathUtils {

  private PathUtils() {
    // util class
  }

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
   * Join each element in paths in order, separated by {@code TachyonURI.SEPARATOR}.
   * <p>
   * For example,
   *
   * <pre>
   * {@code
   * concatPath("/myroot/", "dir", 1L, "filename").equals("/myroot/dir/1/filename");
   * concatPath("tachyon://myroot", "dir", "filename").equals("tachyon://myroot/dir/filename");
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
        CharMatcher.is(TachyonURI.SEPARATOR.charAt(0)).trimTrailingFrom(base.toString().trim());
    trimmedPathList.add(trimmedBase);
    for (Object path : paths) {
      if (null == path) {
        continue;
      }
      String trimmedPath =
          CharMatcher.is(TachyonURI.SEPARATOR.charAt(0)).trimFrom(path.toString().trim());
      if (!trimmedPath.isEmpty()) {
        trimmedPathList.add(trimmedPath);
      }
    }
    if (trimmedPathList.size() == 1 && trimmedBase.isEmpty()) {
      // base must be "[/]+"
      return TachyonURI.SEPARATOR;
    }
    return Joiner.on(TachyonURI.SEPARATOR).join(trimmedPathList);

  }

  /**
   * Get the parent of the file at a path.
   *
   * @param path The path
   * @return the parent path of the file; this is "/" if the given path is the root.
   * @throws InvalidPathException if the path is invalid
   */
  public static String getParent(String path) throws InvalidPathException {
    String cleanedPath = cleanPath(path);
    String name = FilenameUtils.getName(cleanedPath);
    String parent = cleanedPath.substring(0, cleanedPath.length() - name.length() - 1);
    if (parent.isEmpty()) {
      // The parent is the root path
      return TachyonURI.SEPARATOR;
    }
    return parent;
  }

  /**
   * Get the path components of the given path.
   *
   * @param path The path to split
   * @return the path split into components
   * @throws InvalidPathException if the path is invalid
   */
  public static String[] getPathComponents(String path) throws InvalidPathException {
    path = cleanPath(path);
    if (isRoot(path)) {
      String[] ret = new String[1];
      ret[0] = "";
      return ret;
    }
    return path.split(TachyonURI.SEPARATOR);
  }

  /**
   * Check if the given path is the root.
   *
   * @param path The path to check
   * @return true if the path is the root
   * @throws InvalidPathException if the path is invalid
   */
  public static boolean isRoot(String path) throws InvalidPathException {
    return TachyonURI.SEPARATOR.equals(cleanPath(path));
  }

  /**
   * Check if the given path is properly formed.
   *
   * @param path The path to check
   * @throws InvalidPathException If the path is not properly formed
   */
  public static void validatePath(String path) throws InvalidPathException {
    if (path == null || path.isEmpty() || !path.startsWith(TachyonURI.SEPARATOR)
        || path.contains(" ")) {
      throw new InvalidPathException("Path " + path + " is invalid.");
    }
  }

  /**
   * Creates a unique path based off the caller.
   *
   * @return unique path based off the caller
   */
  public static final String uniqPath() {
    StackTraceElement caller = new Throwable().getStackTrace()[1];
    long time = System.nanoTime();
    return "/" + caller.getClassName() + "/" + caller.getMethodName() + "/" + time;
  }
}
