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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.util.OSUtils;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FilenameUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities related to both Alluxio paths like {@link AlluxioURI} and local file paths.
 */
@ThreadSafe
public final class PathUtils {
  private static final String TEMPORARY_SUFFIX_FORMAT = ".alluxio.0x%016X.tmp";
  private static final int TEMPORARY_SUFFIX_LENGTH =
      String.format(TEMPORARY_SUFFIX_FORMAT, 0).length();
  private static final CharMatcher SEPARATOR_MATCHER =
      CharMatcher.is(AlluxioURI.SEPARATOR.charAt(0));

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
   * Joins two path elements, separated by {@link AlluxioURI#SEPARATOR}.
   * <p>
   * Note that empty element in base or paths is ignored.
   *
   * @param base base path
   * @param path path element to concatenate
   * @return joined path
   */
  public static String concatPath(Object base, Object path) {
    Preconditions.checkNotNull(base, "base");
    Preconditions.checkNotNull(path, "path");
    String trimmedBase = SEPARATOR_MATCHER.trimTrailingFrom(base.toString());
    String trimmedPath = SEPARATOR_MATCHER.trimFrom(path.toString());

    StringBuilder output = new StringBuilder(trimmedBase.length() + trimmedPath.length() + 1);
    output.append(trimmedBase);
    if (!trimmedPath.isEmpty()) {
      output.append(AlluxioURI.SEPARATOR);
      output.append(trimmedPath);
    }

    if (output.length() == 0) {
      // base must be "[/]+"
      return AlluxioURI.SEPARATOR;
    }
    return output.toString();
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
    String trimmedBase = SEPARATOR_MATCHER.trimTrailingFrom(base.toString());

    StringBuilder output = new StringBuilder();
    output.append(trimmedBase);
    for (Object path : paths) {
      if (path == null) {
        continue;
      }
      String trimmedPath = SEPARATOR_MATCHER.trimFrom(path.toString());
      if (!trimmedPath.isEmpty()) {
        output.append(AlluxioURI.SEPARATOR);
        output.append(trimmedPath);
      }
    }
    if (output.length() == 0) {
      // base must be "[/]+"
      return AlluxioURI.SEPARATOR;
    }
    return output.toString();
  }

  /**
   * @param paths the set of paths
   * @return the lowest common ancestor, or null if paths is null or empty
   */
  public static AlluxioURI findLowestCommonAncestor(Collection<AlluxioURI> paths) {
    if (paths == null || paths.isEmpty()) {
      return null;
    }
    List<String> matchedComponents = null;
    int matchedLen = 0;
    for (AlluxioURI path : paths) {
      String[] pathComp = path.getPath().split(AlluxioURI.SEPARATOR);
      if (matchedComponents == null) {
        matchedComponents = new ArrayList<>(Arrays.asList(pathComp));
        matchedLen = pathComp.length;
      }

      for (int i = 0; i < pathComp.length && i < matchedLen; ++i) {
        if (!matchedComponents.get(i).equals(pathComp[i])) {
          matchedLen = i;
          break;
        }
      }
    }
    return new AlluxioURI(PathUtils.concatPath(AlluxioURI.SEPARATOR,
        matchedComponents.subList(0, matchedLen).toArray()));
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
      // The parent is the root path.
      return AlluxioURI.SEPARATOR;
    }
    return parent;
  }

  /**
   * Join two path elements for ufs, separated by {@link AlluxioURI#SEPARATOR}.
   *
   * For example,
   *
   * <pre>
   * {@code
   * concatUfsPath("s3://myroot/", "filename").equals("s3://myroot/filename");
   * concatUfsPath("s3://", "filename").equals("s3://filename");
   * }
   * </pre>
   *
   * @param base base path
   * @param path path element to concatenate
   * @return joined path
   */
  public static String concatUfsPath(String base, String path) {
    Pattern basePattern = Pattern.compile("(...)\\/\\/");
    // use conCatPath to join path if base is not starting with //
    if (!basePattern.matcher(base).matches()) {
      return concatPath(base, path);
    } else {
      Preconditions.checkArgument(base != null, "Failed to concatPath: base is null");
      Preconditions.checkArgument(path != null, "Failed to concatPath: a null set of paths");
      String trimmedPath = SEPARATOR_MATCHER.trimFrom(path);
      StringBuilder output = new StringBuilder(base.length() + trimmedPath.length());
      output.append(base);
      output.append(trimmedPath);
      return output.toString();
    }
  }

  /**
   * Get temp path for async persistence job.
   *
   * @param path ufs path
   * @return ufs temp path with UUID
   */
  public static String getPersistentTmpPath(String path) {
    StringBuilder tempFilePath = new StringBuilder();
    StringBuilder tempFileName = new StringBuilder();
    String fileName = FilenameUtils.getName(path);
    String timeStamp = String.valueOf(System.currentTimeMillis());
    tempFilePath.append(".alluxio_ufs_persistence/");
    String uuid = UUID.randomUUID().toString();
    tempFileName.append(fileName);
    tempFileName.append(".alluxio.");
    tempFileName.append(timeStamp);
    tempFileName.append(String.format(".%s", uuid));
    tempFileName.append(".tmp");
    tempFilePath.append(tempFileName);
    return tempFilePath.toString();
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
   * Removes the prefix from the path, yielding a relative path from the second path to the first.
   *
   * If the paths are the same, this method returns the empty string.
   *
   * @param path the full path
   * @param prefix the prefix to remove
   * @return the path with the prefix removed
   * @throws InvalidPathException if either of the arguments are not valid paths
   */
  public static String subtractPaths(String path, String prefix) throws InvalidPathException {
    String cleanedPath = cleanPath(path);
    String cleanedPrefix = cleanPath(prefix);
    if (cleanedPath.equals(cleanedPrefix)) {
      return "";
    }
    if (!hasPrefix(cleanedPath, cleanedPrefix)) {
      throw new RuntimeException(
          String.format("Cannot subtract %s from %s because it is not a prefix", prefix, path));
    }
    // The only clean path which ends in '/' is the root.
    int prefixLen = cleanedPrefix.length();
    int charsToDrop = PathUtils.isRoot(cleanedPrefix) ? prefixLen : prefixLen + 1;
    return cleanedPath.substring(charsToDrop, cleanedPath.length());
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
    boolean invalid = (path == null || path.isEmpty());
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
    return path + String.format(TEMPORARY_SUFFIX_FORMAT, nonce);
  }

  /**
   * @param path the path of the file, possibly temporary
   * @return the permanent path of the file if it was temporary, or the original path if it was not
   */
  public static String getPermanentFileName(String path) {
    if (isTemporaryFileName(path)) {
      return path.substring(0, path.length() - TEMPORARY_SUFFIX_LENGTH);
    }
    return path;
  }

  /**
   * Determines whether the given path is a temporary file name generated by Alluxio.
   *
   * @param path the path to check
   * @return whether the given path is a temporary file name generated by Alluxio
   */
  public static boolean isTemporaryFileName(String path) {
    return path.matches("^.*\\.alluxio\\.0x[0-9A-F]{16}\\.tmp$");
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
   * Adds a trailing separator if it does not exist in path.
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
