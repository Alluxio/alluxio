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
 * Abstraction of all types of path in Tachyon lexically and syntactically
 * <p>
 * <p>
 * Path in Tachyon:
 * <ul>
 * <li>Tachyon path: tachyon://host:port/path, tachyon-ft://host:port/path</li>
 * <li>UnderFileSystem path: scheme(hdfs, s3, etc.)://host:port/path, file:///path</li>
 * </ul>
 * <p>
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
public class Path implements Comparable<Path> {
  private static final String UNIX_PATH_SEPARATOR = "/";
  private static final String WINDOWS_PATH_SEPARATOR = "\\";
  private static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  private String mScheme = null;

  private String mHost = null;

  private int mPort = -1;

  private String mPath = null;

  /**
   * Constructs Path from String with thorough legality validation
   * 
   * @param path
   *          Raw String path
   * @throws IllegalArgumentException
   *           specific error information about the illegality of path
   */
  public Path(String path) throws IllegalArgumentException {
    if (path == null) {
      throw new IllegalArgumentException("path parameter can not be null");
    } else if (path.contains(" ")) {
      throw new IllegalArgumentException("path parameter " + path + " can not contain space");
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
    String scheme = null;
    int colon = path.indexOf(":");
    if (colon != -1 && path.substring(colon + 1, colon + 3) == "//") {
      scheme = path.substring(0, colon);
    }
    if (scheme == null) {
      throw new IllegalArgumentException("path parameter " + path + " must have a scheme");
    }

    // get authority(host:port)
    String authority = null;
    String host = null;
    int port = -1;
    int start = colon + 3;
    int nextColon = path.indexOf(":", start);
    int nextSlash = path.indexOf("/", start);
    if (nextColon != -1 && nextSlash != -1 && nextColon < nextSlash) {
      host = path.substring(start, nextColon);
      try {
        port = Integer.parseInt(path.substring(nextColon + 1, nextSlash));
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException("path parameter " + path
            + " port element should be Integer");
      }
      authority = host + ":" + Integer.toString(port);
    }

    // scheme 'file://' refers to local underFileSystem, should not contain authority,
    // other schemes must contain authority
    if (scheme.equals("file") && authority != null) {
      throw new IllegalArgumentException("path parameter " + path
          + " is local, no authority needed");
    } else if (!scheme.equals("file") && authority == null) {
      throw new IllegalArgumentException("path parameter " + path + " must contain authority");
    }

    // remaining part of 'path' after authority is the common file system path
    // we don't check filePath's legality, it should be checked its corresponding file system
    String filePath = path.substring(nextSlash + 1);

    mScheme = scheme;
    mHost = host;
    mPort = port;

    // normalize path, remove redundant '..', '.'
    mPath = FilenameUtils.normalize(filePath);

  }

  public Path(String scheme, String authority, String path) {
    this(scheme + "://" + authority + "/" + path);
  }

  /**
   * Assert paths have the same scheme and authority
   * 
   * @param paths
   *          paths to be compared
   * @throws java.lang.IllegalArgumentException
   *           If there exist two paths who don't share the same scheme and authority or length
   *           of parameter is 0
   */
  private void assertSameSchemeAndAuthority(Path... paths) throws IllegalArgumentException {
    if (paths.length == 0) {
      throw new IllegalArgumentException("parameter paths can not be empty");
    }
    if (paths.length < 2) {
      return;
    }
    String scheme = paths[0].getScheme();
    String authority = paths[0].getAuthority();
    for (int i = 1; i < paths.length; i ++) {
      if ((!paths[i].getScheme().equals(scheme)) || (!paths[i].getAuthority().equals(authority))) {
        throw new IllegalArgumentException("parameters should be of the same scheme and authority");
      }
    }
  }

  /**
   * Compare this Path to another Path
   * <p/>
   * <p>
   * When comparing corresponding components of two Paths, if one component is undefined but the
   * other is defined then the first is considered to be less than the second. Unless otherwise
   * noted, string components are ordered according to their natural, case-sensitive ordering as
   * defined by the {@link java.lang.String#compareTo(Object)
   * String.compareTo} method. String components that are subject to encoding are compared by
   * comparing their raw forms rather than their encoded forms.
   * </p>
   * <p>
   * The ordering of Paths is defined as follows:
   * </p>
   * <p/>
   * <ul type=disc>
   * <p/>
   * <li>
   * <p>
   * Two Paths with different schemes are ordered according the ordering of their schemes, without
   * regard to case.
   * </p>
   * </li>
   * <p/>
   * <li>
   * <p>
   * Two Paths with identical schemes are ordered according to the ordering of their authority
   * components: first, Paths are ordered according to the ordering of their hosts, without regard
   * to case; if the hosts are identical then the Paths are ordered according to the ordering of
   * their ports.
   * </p>
   * </li>
   * <p/>
   * <li>
   * <p>
   * Finally, two Paths with identical schemes and authority components are ordered according to the
   * ordering of their paths
   * <p/>
   * </ul>
   * <p/>
   * <p>
   * This method satisfies the general contract of the
   * {@link java.lang.Comparable#compareTo(Object) Comparable.compareTo} method.
   * </p>
   * 
   * @param other
   *          The path to which this Path is to be compared
   * @return A negative integer, zero, or a positive integer as this Path is less than,
   *         equal to, or greater than the given Path
   */
  @Override
  public int compareTo(Path other) {
    int ret = 0;
    // compare scheme
    ret = getScheme().compareTo(other.getScheme());
    if (ret != 0) {
      return ret;
    }
    // compare authority
    String auth1 = getAuthority();
    String auth2 = other.getAuthority();
    if (auth1 == null && auth2 != null) {
      return -1;
    } else if (auth1 != null && auth2 == null) {
      return 1;
    } else if (auth1 != null && auth2 != null) {
      ret = getHost().compareTo(other.getHost());
      if (ret != 0) {
        return ret;
      }
      int port1 = getPort();
      int port2 = other.getPort();
      if (port1 < port2) {
        return -1;
      } else if (port1 > port2) {
        return 1;
      }
    }
    // compare path
    ret = getPath().compareTo(other.getPath());
    return ret;
  }

  /**
   * Number of elements in path components of the Path
   * <p/>
   * 
   * <pre>
   * /                                  -> 0
   * /a                                 -> 1
   * /a/b/c.txt                         -> 3
   * /a/b                               -> 2
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
    if (!(other instanceof Path)) {
      return false;
    }
    Path p = (Path) other;
    return compareTo(p) == 0;
  }

  /**
   * Get authority component
   * <p/>
   * authority component consists of host:port
   */
  public String getAuthority() {
    return getHost() + ":" + Integer.toString(getPort());
  }

  /**
   * Gets the extension of a filename.
   * <p/>
   * This method returns the textual part of the filename after the last dot. There must be no
   * directory separator after the dot.
   * 
   * <pre>
   * foo.txt                                --> "txt"
   * file://a/b/c.jpg                       --> "jpg"
   * tachyon://localhost:19998/a/b.txt/c    --> ""
   * a/b/c                                  --> ""
   * </pre>
   * <p/>
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
   * <p/>
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
   * <p/>
   * The output will be the same irrespective of the machine that the code is running on.
   * 
   * @return the name of the file without the path, or an empty string if none exists
   */
  public String getFileNameWithExtension() {
    return FilenameUtils.getName(getPath());
  }

  /**
   * Gets the base name, minus the full path and extension, from a full filename.
   * <p/>
   * This method will handle a file in either Unix or Windows format. The text after the last
   * forward or backslash and before the last dot is returned.
   * 
   * <pre>
   * a/b/c.txt --> c
   * a.txt     --> a
   * a/b/c     --> c
   * a/b/c/    --> ""
   * tachyon://localhost:1998/a/b/c.txt --> c
   * file://a.txt     --> a
   * hdfs://localhost:1999/a/b/c/    --> ""
   * </pre>
   * <p/>
   * The output will be the same irrespective of the machine that the code is running on.
   * 
   * @return the name of the file without the path, or an empty string if none exists
   */
  public String getFileNameWithoutExtension() {
    return FilenameUtils.getBaseName(getPath());
  }

  /**
   * Get the host of authority
   * <p/>
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
   * <p/>
   * 
   * <pre>
   * scheme://authority/a      -> scheme://authority/
   * scheme://authority/       -> null
   * scheme://authority/C:\a\b -> scheme://authority/C:\a
   * scheme://authority/C:\    -> null
   * </pre>
   */
  public Path getParent() {
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    String parentPath = path.substring(0, path.lastIndexOf("/"));
    parentPath = FilenameUtils.separatorsToSystem(parentPath);
    if (parentPath.isEmpty()) {
      return null;
    } else {
      return new Path(getScheme(), getAuthority(), parentPath);
    }
  }

  /**
   * Get the path component of Path
   * <p/>
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
   * Get the port of authority
   * <p/>
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
   * <p/>
   * <p/>
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
   * <p/>
   * <p/>
   * The output will be the same irrespective of the machine that the code is running on. ie. both
   * Unix and Windows prefixes are matched regardless.
   */
  public Path getRoot() {
    String path = getPath();
    String rootPath = FilenameUtils.getPrefix(path);
    return new Path(getScheme(), getAuthority(), rootPath);
  }

  /**
   * Get the scheme component of Path
   * <p/>
   * scheme://authority/path -> scheme scheme://path -> scheme
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
   * <p/>
   * 
   * <pre>
   * Windows:
   * C:\a -> absolute
   * C:a  -> relative
   * <p/>
   * Unix:
   * /a/b -> absolute
   * </pre>
   * 
   * @return <code>true</code> if the path component is absolute else <code>false</code>
   */
  public boolean isAbsolute() {
    Path root = null;
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
   * Join paths to new Path, scheme and authority should all be the same
   * 
   * @param others
   *          other Paths to sequentially append to base path
   * @return joined Path
   * @throws java.lang.IllegalArgumentException
   *           if parameters do not share the same scheme and authority
   */
  public Path join(Path... others) throws IllegalArgumentException {
    assertSameSchemeAndAuthority(others);
    return join(pathsToStringpaths(others));
  }

  /**
   * Join paths to new Path, scheme and authority be the same with first parameter path
   * 
   * @param others
   *          other other common local file system paths
   * @return joined Path
   */
  public Path join(String... others) {
    String joinedPath = getPath();
    for (String other : others) {
      joinedPath = FilenameUtils.concat(joinedPath, other);
    }
    return new Path(getScheme(), getAuthority(), joinedPath);
  }

  /**
   * Composite path components of parameter paths to a String list
   * 
   * @param paths
   *          Paths to be composed from
   * @return composed string list
   */
  private String[] pathsToStringpaths(Path... paths) {
    String[] ret = new String[paths.length];
    for (int i = 0; i < ret.length; i ++) {
      ret[i] = paths[i].getPath();
    }
    return ret;
  }

  /**
   * Equivallent as relative(other.getPath())
   * 
   * @param other
   *          the path to relativize against this path,
   *          share same scheme and authority with the caller
   * @return relative Path
   * @throws java.lang.IllegalArgumentException
   *           if other do not share the same scheme and authority with the caller
   */
  public Path relativize(Path other) throws IllegalArgumentException {
    if (getScheme().equals(other.getScheme()) && getAuthority().equals(other.getAuthority())) {
      return relativize(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and authority with the caller");
    }
  }

  /**
   * Constructs a relative path between this path and a given path.
   * <p/>
   * <p>
   * Relativization is the inverse of resolution. This method attempts to construct a relative path
   * that when resolved against this path, yields a path that locates the same file as the given
   * path. That is: p.resolve(p.relativize(q)) == q
   * <p/>
   * 
   * <pre>
   * Example:
   * "/a/b".relativize("/a/b/c/d") = "c/d"
   * "/a/b".relativize("/a/c") = "../c"
   * </pre>
   * <p/>
   * Attention: the two paths must all be absolute paths, if not, empty path is returned if the
   * caller path is longer than the other, empty path is returned if the caller path is the same
   * with the other, empty path is returned
   * </p>
   * 
   * @param other
   *          the local file system path to relativize against this path
   * @return relative Path
   */
  public Path relativize(String other) {
    String path = getPath();
    path = FilenameUtils.separatorsToUnix(path);
    other = FilenameUtils.separatorsToUnix(other);
    Path otherPath = new Path("file", null, other);
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

    return new Path(getScheme(), getAuthority(), relativePath);
  }

  /**
   * Equivallent as resolve(other.getPath())
   * 
   * @param other
   *          the path string to resolve against this path,
   *          share same scheme and authority with the caller
   * @return the resulting Path
   * @throws IllegalArgumentException
   *           if the two Path don't share the same scheme and authority
   */
  public Path resolve(Path other) throws IllegalArgumentException {
    if (getScheme().equals(other.getScheme()) && getAuthority().equals(other.getAuthority())) {
      return resolve(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and authority with the caller");
    }
  }

  /**
   * Resolve the given path against this path.
   * <p/>
   * Simply join them together
   * 
   * @param other
   *          the path to resolve against this path
   * @return the resulting path
   */
  public Path resolve(String other) {
    return join(other);
  }

  /**
   * Equivalent as resoveSibling(other.getPath())
   * 
   * @param other
   *          the path to resolve against this path's parent
   * @return the resulting Path
   * @throws IllegalArgumentException
   *           if the two paths do not share the same scheme and authority
   */
  public Path resolveSibling(Path other) throws IllegalArgumentException {
    if (getScheme().equals(other.getScheme()) && getAuthority().equals(other.getAuthority())) {
      return resolveSibling(other.getPath());
    } else {
      throw new IllegalArgumentException("paramter other must share the same"
          + " scheme and authority with the caller");
    }
  }

  /**
   * Resolves the given path against this path's parent path
   * <p/>
   * Simply join <code>other</code> to this path's parent path
   * 
   * @param other
   *          the path to resolve against this path's parent
   * @return the resulting path
   */
  public Path resolveSibling(String other) {
    return join(other);
  }

  /**
   * Check whether the path starts with Windows Drive Specifier "[a-zA-Z]:"
   * assume scheme's length should be > 1
   */
  private boolean startsWithWindowsDrive(String path) {
    char driveName = path.charAt(0);
    boolean driveNameValid =
        (driveName >= 'a' && driveName <= 'z') || (driveName >= 'A' && driveName <= 'Z');
    String driveSpecifier = path.substring(1, 2);
    return driveSpecifier == ":" && driveNameValid;
  }

  /**
   * path component of Path object contains elements seperated by SEPERATOR('/' or '\')
   * This method gets path's [beginIndex, endIndex) elements and construct a new Path object
   * <p/>
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
  public Path subpath(int beginIndex, int endIndex) throws IllegalArgumentException {
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

    return new Path(getScheme(), getAuthority(), subPath);
  }

  @Override
  public String toString() {
    return getScheme() + "://" + getAuthority() + "/" + getPath();
  }
}
