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

import org.apache.commons.io.FilenameUtils;

/**
 * Abstraction of all types of path in Tachyon lexically and syntactically.
 * <p>
 * Path in Tachyon:
 * <ul>
 * <li>Tachyon path: tachyon://host:port/path, tachyon-ft://host:port/path</li>
 * <li>UnderFileSystem path: scheme(hdfs, s3, etc.)://host:port/path, file:///path</li>
 * </ul>
 * <p>
 * Path in Tachyon has the following components:
 * <ul>
 * <li>scheme(MUST)</li>
 * <li>host:port(OPTIONAL) - if address is omitted, the path representation becomes scheme:///path,
 * pay attention to the third slash.</li>
 * <li>path(MUST) - Windows path should be kept as it is on a Windows Platform, this Class will take
 * care of both Unix and Window path representation, e.g. scheme://host:port/a\b represents relative
 * path a\b on Windows, or scheme://host:port/C:\ represents root directory of Driver C on Windows.
 * For Unix path, scheme://host:port//a/b represents /a/b, scheme://host:port/a/b represents a/b</li>
 * </ul>
 */
public class Path2 implements Comparable<Path2> {
  private static final String UNIX_PATH_SEPARATOR = "/";
  private static final String WINDOWS_PATH_SEPARATOR = "\\";
  private static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  private String mScheme = null;
  private String mHost = null;
  private int mPort = -1;
  private String mPath = null;

  /**
   * Constructs Path from String with legality validation
   * 
   * @param path
   *          Path in String representation
   * @throws IllegalArgumentException
   *           specific error information about the illegality of path
   */
  public Path2(String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path can not be null");
    }

    // add scheme 'file' to bare local file system path
    // Unix:
    // /a/b/c -> file:////a/b/c
    //
    // Windows:
    // C:\a\b -> file:///C:\a\b
    // C:a\b -> file:///C:a\b
    // \a\b -> file:///\a\b
    // a\b -> file:///a\b
    boolean startsWithDrive = startsWithWindowsDrive(path);
    if (startsWithDrive || !path.contains(":")) {
      path = "file:///" + path;
    }

    // get scheme, path should start with "scheme://"
    int colon = path.indexOf(":");
    if (colon != -1 && path.substring(colon + 1, colon + 3).equals("//")) {
      mScheme = path.substring(0, colon);
    }
    if (mScheme == null) {
      throw new IllegalArgumentException("Path " + path + " must have a scheme");
    }

    // get address(host:port)
    int start = colon + 3;
    int nextColon = path.indexOf(":", start);
    int nextSlash = path.indexOf("/", start);
    if (nextColon != -1 && nextSlash != -1 && nextColon < nextSlash) {
      mHost = path.substring(start, nextColon);
      try {
        mPort = Integer.parseInt(path.substring(nextColon + 1, nextSlash));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Path " + path + " port element is not integer."
            + e.getMessage());
      }
    }

    // Scheme 'file://' refers to local underFileSystem, should not contain address, other schemes
    // must contain address
    if (mScheme.equals("file") && mHost != null) {
      throw new IllegalArgumentException("Path " + path + " is local, no address needed");
    } else if (!mScheme.equals("file") && mHost == null) {
      throw new IllegalArgumentException("Path " + path + " must contain address");
    }

    // Remaining part of 'path' after address is the common file system path
    // We don't check filePath's legality, it should be checked its corresponding file system
    String filePath = path.substring(nextSlash + 1);

    // normalize path, remove redundant '..', '.'
    mPath = FilenameUtils.normalize(filePath);
  }

  /**
   * Constructs Paths with scheme, address, and path;
   * 
   * @param scheme
   *          the scheme of the path, e.g. tachyon
   * @param address
   *          the address of the path, e.g. localhost:19998
   * @param path
   *          the path of the file, e.g. /a/b/c
   * @throws IllegalArgumentException
   */
  public Path2(String scheme, String address, String path) {
    this(scheme + "://" + address + "/" + path);
  }

  /**
   * Assert paths have the same scheme and address
   * 
   * @param paths
   *          paths to be compared
   * @throws IllegalArgumentException
   *           If there exist two paths who don't share the same scheme and address or length
   *           of parameter is 0
   */
  private void assertSameSchemeAndAddress(Path2... paths) {
    if (paths.length == 0) {
      throw new IllegalArgumentException("Parameter paths can not be empty");
    }
    if (paths.length < 2) {
      return;
    }
    String scheme = paths[0].getScheme();
    String address = paths[0].getAddress();
    for (int i = 1; i < paths.length; i ++) {
      if ((!paths[i].getScheme().equals(scheme)) || (!paths[i].getAddress().equals(address))) {
        throw new IllegalArgumentException("parameters should be of the same scheme and address");
      }
    }
  }

  /**
   * Compare this Path to another Path based on the String representation of the two Paths.
   * 
   * @param other
   *          The path to which this Path is to be compared
   * @return A negative integer, zero, or a positive integer as this Path is less than,
   *         equal to, or greater than the given Path
   */
  @Override
  public int compareTo(Path2 other) {
    return toString().compareTo(other.toString());
  }

  /**
   * Number of elements in path components of the Path
   * 
   * <pre>
   * /                                  -> 0
   * /a                                 -> 1
   * /a/b/c.txt                         -> 3
   * /a/b/                              -> 3
   * a/b                                -> 2
   * a\b                                -> 2
   * C:\a                               -> 2
   * C:                                 -> 1
   * tachyon://localhost:1998/          -> 0
   * tachyon://localhost:1998/a         -> 1
   * tachyon://localhost:1998/a/b.txt   -> 2
   * </pre>
   */
  public int depth() {
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    if (!path.startsWith(UNIX_PATH_SEPARATOR)) {
      path = UNIX_PATH_SEPARATOR + path;
    }
    int depth = 0;
    int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
    while (slash != -1) {
      depth ++;
      slash = path.indexOf(UNIX_PATH_SEPARATOR, slash + 1);
    }
    return depth;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Path2)) {
      return false;
    }
    Path2 p = (Path2) other;
    return compareTo(p) == 0;
  }

  /**
   * Get address component
   * <p>
   * address component consists of host:port
   */
  public String getAddress() {
    return getHost() + ":" + Integer.toString(getPort());
  }

  /**
   * Gets the extension of a filename.
   * <p>
   * This method returns the textual part of the filename after the last dot. There must be no
   * directory separator after the dot.
   * 
   * <pre>
   * foo.txt                                --> "txt"
   * file://a/b/c.jpg                       --> "jpg"
   * tachyon://localhost:19998/a/b.txt/c    --> ""
   * a/b/c                                  --> ""
   * </pre>
   * <p>
   * The output will be the same irrespective of the machine that the code is running on.
   * 
   * @return the extension of the file or an empty string if none exists or {@code null} if the
   *         filename is {@code null}.
   */
  public String getExtension() {
    return FilenameUtils.getExtension(getPath());
  }

  /**
   * Gets the name minus the path from a full filename.
   * <p>
   * This method will handle a file in either Unix or Windows format. The text after the last
   * forward or backslash is returned.
   * 
   * <pre>
   * a/b/c.txt                          --> c.txt
   * a.txt                              --> a.txt
   * a/b/c                              --> c
   * a/b/c/                             --> ""
   * file:///a/b/c.txt                  --> c.txt
   * file://a/b/c.txt                   --> c.txt
   * tachyon://localhost:1998/a.txt     --> a.txt
   * </pre>
   * <p>
   * The output will be the same irrespective of the machine that the code is running on.
   * 
   * @return the name of the file without the path, or an empty string if none exists
   */
  public String getFileNameWithExtension() {
    return FilenameUtils.getName(getPath());
  }

  /**
   * Gets the base name, minus the full path and extension, from a full filename.
   * <p>
   * This method will handle a file in either Unix or Windows format. The text after the last
   * forward or backslash and before the last dot is returned.
   * 
   * <pre>
   * a/b/c.txt                          --> c
   * a.txt                              --> a
   * a/b/c                              --> c
   * a/b/c/                             --> ""
   * tachyon://localhost:1998/a/b/c.txt --> c
   * file://a.txt                       --> a
   * hdfs://localhost:1999/a/b/c/       --> ""
   * </pre>
   * <p>
   * The output will be the same irrespective of the machine that the code is running on.
   * 
   * @return the name of the file without the path, or an empty string if none exists
   */
  public String getFileNameWithoutExtension() {
    return FilenameUtils.getBaseName(getPath());
  }

  /**
   * Get the host of address
   * <p>
   * 
   * <pre>
   * hdfs://localhost:19999/a/b    -> localhost
   * tachyon://127.0.0.1:1998/a    -> 127.0.0.1
   * file:///C:\a\b                -> null
   * </pre>
   */
  public String getHost() {
    return mHost;
  }

  /**
   * Construct new Path whose path component is this Path's path's parent path
   * <p>
   * 
   * <pre>
   * scheme://address/a      -> scheme://address/
   * scheme://address/       -> null
   * scheme://address/C:\a\b -> scheme://address/C:\a
   * scheme://address/C:\    -> null
   * </pre>
   */
  public Path2 getParent() {
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    String parentPath = path.substring(0, path.lastIndexOf("/"));
    parentPath = FilenameUtils.separatorsToSystem(parentPath);
    if (parentPath.isEmpty()) {
      return null;
    } else {
      return new Path2(getScheme(), getAddress(), parentPath);
    }
  }

  /**
   * Get the path component of Path
   * <p>
   * 
   * <pre>
   * scheme://host:port//a/b         -> /a/b
   * scheme://host:port/a/b          -> a/b
   * scheme://host:port/../../a/b    -> ../../a/b
   * scheme://host:port/C:\a\        -> C:\a\
   * file:///a.txt                   -> a.txt
   * file:///C:\a.txt                -> C:\a.txt
   * scheme://host:port/             -> ""
   * </pre>
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get the port of address
   * <p>
   * 
   * <pre>
   * hdfs://localhost:19999/a/b    -> 19999
   * tachyon://127.0.0.1:1998/a    -> 1998
   * file:///C:\a\b                -> -1
   * </pre>
   */
  public int getPort() {
    return mPort;
  }

  /**
   * Construct new Path whose path component is this Path's path's root,
   * other components not changed
   * <p>
   * This method will handle a file in either Unix or Windows format. The prefix includes the first
   * slash in the full filename where applicable.
   * 
   * <pre>
   * Windows:
   * a\b\c.txt           --> ""          --> relative
   * \a\b\c.txt          --> "\"         --> current drive absolute
   * C:a\b\c.txt         --> "C:"        --> drive relative
   * C:\a\b\c.txt        --> "C:\"       --> absolute
   * \\server\a\b\c.txt  --> "\\server\" --> UNC
   * 
   * Unix:
   * a/b/c.txt           --> ""          --> relative
   * /a/b/c.txt          --> "/"         --> absolute
   * ~/a/b/c.txt         --> "~/"        --> current user
   * ~                   --> "~/"        --> current user (slash added)
   * ~user/a/b/c.txt     --> "~user/"    --> named user
   * ~user               --> "~user/"    --> named user (slash added)
   * </pre>
   * <p>
   * The output will be the same irrespective of the machine that the code is running on. ie. both
   * Unix and Windows prefixes are matched regardless.
   */
  public Path2 getRoot() {
    String path = getPath();
    String rootPath = FilenameUtils.getPrefix(path);
    return new Path2(getScheme(), getAddress(), rootPath);
  }

  /**
   * Get the scheme component of Path
   * <p>
   * scheme://address/path -> scheme scheme://path -> scheme
   */
  public String getScheme() {
    return mScheme;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  /**
   * Whether path component of this Path is Absolute
   * <p>
   * 
   * <pre>
   * Windows:
   * C:\a -> absolute
   * C:a  -> relative
   * 
   * Unix:
   * /a/b -> absolute
   * </pre>
   * 
   * @return <code>true</code> if the path component is absolute else <code>false</code>
   */
  public boolean isAbsolute() {
    Path2 root = null;
    try {
      root = getRoot();
    } catch (IllegalArgumentException iae) {
      return false;
    }
    String path = root.getPath();
    if (!WINDOWS && path.startsWith("/") || WINDOWS && startsWithWindowsDrive(path)
        && path.substring(2, 3).equals(WINDOWS_PATH_SEPARATOR)) {
      return true;
    }
    return false;
  }

  /**
   * Whether the path component is a root path
   * 
   * @return <code>true</code> if the path component is root else <code>false</code>
   */
  public boolean isRoot() {
    return equals(getRoot());
  }

  /**
   * Join paths to new Path, scheme and address should all be the same
   * 
   * @param others
   *          other Paths to be sequentially appended to base path
   * @return joined Path
   * @throws IllegalArgumentException
   *           if parameters do not share the same scheme and address
   */
  public Path2 join(Path2... others) {
    assertSameSchemeAndAddress(others);
    return join(pathsToStringpaths(others));
  }

  /**
   * Join paths to new Path, scheme and address be the same with this Path
   * 
   * @param others
   *          other common local file system paths
   * @return joined Path
   */
  public Path2 join(String... others) {
    String joinedPath = getPath();
    for (String other : others) {
      joinedPath = FilenameUtils.concat(joinedPath, other);
    }
    return new Path2(getScheme(), getAddress(), joinedPath);
  }

  /**
   * Get a list of path components from <code>paths</code>
   * 
   * @param paths
   *          Paths to get path components from
   * @return String list of path components
   */
  private String[] pathsToStringpaths(Path2... paths) {
    String[] ret = new String[paths.length];
    for (int i = 0; i < ret.length; i ++) {
      ret[i] = paths[i].getPath();
    }
    return ret;
  }

  /**
   * Equivalent as relative(other.getPath())
   * 
   * @param other
   *          the path to relativize against this path,
   *          share same scheme and address with the caller
   * @return relative Path
   * @throws IllegalArgumentException
   *           if <code>>other</code> do not share the same scheme and address with the caller
   */
  public Path2 relativize(Path2 other) {
    if (getScheme().equals(other.getScheme()) && getAddress().equals(other.getAddress())) {
      return relativize(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and address with the caller");
    }
  }

  /**
   * Constructs a relative path between this path and a given path.
   * <p>
   * Relativization is the inverse of resolution. This method attempts to construct a relative path
   * that when resolved against this path, yields a path that locates the same file as the given
   * path. That is: p.resolve(p.relativize(q)) == q
   * 
   * <pre>
   * Example:
   * "/a/b".relativize("/a/b/c/d") = "c/d"
   * "/a/b".relativize("/a/c") = "../c"
   * </pre>
   * <p>
   * Attention: the two paths must all be absolute paths, if not, empty path is returned if the
   * caller path is longer than the other, empty path is returned if the caller path is the same
   * with the other, empty path is returned
   * 
   * @param other
   *          the local file system path to relativize against this path
   * @return relative Path
   * @throws IllegalArgumentException
   */
  public Path2 relativize(String other) {
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    other = FilenameUtils.separatorsToUnix(other);
    Path2 otherPath = new Path2("file", null, other);
    String relativePath = "";

    if (!(path.equals(other) || !isAbsolute() || !otherPath.isAbsolute() || path.length() > other
        .length())) {
      // find the longest match of the two paths
      int index = 1;
      while (index <= path.length()) {
        if (other.startsWith(path.substring(0, index))) {
          ++ index;
        } else {
          break;
        }
      }

      int start = Math.min(index, path.length());
      relativePath = other.substring(start);
      if (relativePath.startsWith("/")) {
        relativePath = relativePath.substring(1);
      }
      String pathRemain = path.substring(start);
      for (int i = 0; i < pathRemain.length(); i ++) {
        if (pathRemain.charAt(i) == '/') {
          relativePath = "../" + relativePath;
        }
      }
    }

    return new Path2(getScheme(), getAddress(), relativePath);
  }

  /**
   * Equivallent as resolve(other.getPath())
   * 
   * @param other
   *          the path string to resolve against this path,
   *          share same scheme and address with the caller
   * @return the resulting Path
   * @throws IllegalArgumentException
   *           if the two Path don't share the same scheme and address
   */
  public Path2 resolve(Path2 other) {
    if (getScheme().equals(other.getScheme()) && getAddress().equals(other.getAddress())) {
      return resolve(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and address with the caller");
    }
  }

  /**
   * Resolve the given path against this path.
   * <p>
   * Simply join them together
   * 
   * @param other
   *          the path to resolve against this path
   * @return the resulting path
   * @throws IllegalArgumentException
   */
  public Path2 resolve(String other) {
    return join(other);
  }

  /**
   * Equivalent as resoveSibling(other.getPath())
   * 
   * @param other
   *          the path to resolve against this path's parent
   * @return the resulting Path
   * @throws IllegalArgumentException
   *           if the two paths do not share the same scheme and address
   */
  public Path2 resolveSibling(Path2 other) {
    if (getScheme().equals(other.getScheme()) && getAddress().equals(other.getAddress())) {
      return resolveSibling(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and address with the caller");
    }
  }

  /**
   * Resolves the given path against this path's parent path
   * <p>
   * Simply join <code>other</code> to this path's parent path
   * 
   * @param other
   *          the path to resolve against this path's parent
   * @return the resulting path
   * @throws IllegalArgumentException
   */
  public Path2 resolveSibling(String other) {
    return join(other);
  }

  /**
   * Check whether the path starts with Windows Drive Specifier "[a-zA-Z]:"
   * assume scheme's length should be > 1
   * 
   * @param path
   *          the path to check
   * @return true if it starts with Windows Drive Specifier "[a-zA-Z]:", false otherwise.
   */
  private boolean startsWithWindowsDrive(String path) {
    if (path.length() < 2) {
      return false;
    }
    char driveName = path.charAt(0);
    boolean driveNameValid =
        (driveName >= 'a' && driveName <= 'z') || (driveName >= 'A' && driveName <= 'Z');
    return driveNameValid && path.charAt(1) == ':';
  }

  /**
   * path component of Path object contains elements seperated by SEPERATOR('/' or '\')
   * This method gets path's [beginIndex, endIndex) elements and construct a new Path object
   * <p>
   * 
   * <pre>
   * "/a/b/c/".subpath(0,0)      = ""
   * "/a/b/c/".subpath(0,1)      = "a"
   * "/a/b/c/".subpath(0,2)      = "a/b"
   * "/a/b/c/".subpath(0,3)      = "a/b/c"
   * "/a/b/c/".subpath(0,4)      = "a/b/c/"
   * "/a/b/c/d.txt".subpath(0,4) = a/b/c/d.txt
   * "/a/b/c/d.txt".subpath(3,4) = d.txt
   * </pre>
   * 
   * @param beginIndex
   *          beginIndex should be in range [ 0, depth() ), else throw exception
   * @param endIndex
   *          endIndex should be in range ( 0, depth() ], else throw exception
   * @return new constructed Path
   */
  public Path2 subpath(int beginIndex, int endIndex) {
    // check index's range
    if (beginIndex < 0 || beginIndex >= depth()) {
      throw new IllegalArgumentException("beginIndex parameter " + beginIndex + " out of range");
    }
    if (endIndex <= 0 || endIndex > depth()) {
      throw new IllegalArgumentException("endIndex parameter " + endIndex + " out of range");
    }
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    int depth = -1;
    int begin = -1, end = -1;
    while (depth <= beginIndex) {
      begin = path.indexOf("/", begin + 1);
      ++ depth;
    }
    end = begin;
    while (depth <= endIndex) {
      end = path.indexOf("/", end + 1);
      ++ depth;
    }
    if (end == -1) {
      end = path.length();
    }
    String subPath = path.substring(begin + 1, end);

    return new Path2(getScheme(), getAddress(), subPath);
  }

  @Override
  public String toString() {
    return getScheme() + "://" + getAddress() + "/" + getPath();
  }
}
