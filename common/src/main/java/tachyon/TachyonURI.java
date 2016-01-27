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

package tachyon;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.lang.StringUtils;

import tachyon.util.OSUtils;

/**
 * It uses a hierarchical URI internally. URI requires that String is escaped, TachyonURI does not.
 *
 * Does not support fragment or query in the URI.
 */
@ThreadSafe
public final class TachyonURI implements Comparable<TachyonURI> {
  public static final String SEPARATOR = "/";
  public static final String CUR_DIR = ".";
  public static final String WILDCARD = "*";

  public static final TachyonURI EMPTY_URI = new TachyonURI("");

  private static final boolean WINDOWS = OSUtils.isWindows();

  // a hierarchical uri
  private final URI mUri;

  /**
   * Construct a TachyonURI from a String. Path strings are URIs, but with unescaped elements and
   * some additional normalization.
   *
   * @param pathStr path to construct the TachyonURI from
   */
  public TachyonURI(String pathStr) {
    if (pathStr == null) {
      throw new IllegalArgumentException("Can not create a uri with a null path.");
    }

    // add a slash in front of paths with Windows drive letters
    if (hasWindowsDrive(pathStr, false)) {
      pathStr = "/" + pathStr;
    }

    // parse uri components
    String scheme = null;
    String authority = null;

    int start = 0;

    // parse uri scheme, if any
    int colon = pathStr.indexOf(':');
    int slash = pathStr.indexOf('/');
    if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
      scheme = pathStr.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathStr.startsWith("//", start) && (pathStr.length() - start > 2)) { // has authority
      int nextSlash = pathStr.indexOf('/', start + 2);
      int authEnd = nextSlash > 0 ? nextSlash : pathStr.length();
      authority = pathStr.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- query & fragment not supported
    String path = pathStr.substring(start, pathStr.length());

    mUri = createURI(scheme, authority, path);
  }

  /**
   * Construct a TachyonURI from components.
   *
   * @param scheme the scheme of the path. e.g. tachyon, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   */
  public TachyonURI(String scheme, String authority, String path) {
    if (path == null) {
      throw new IllegalArgumentException("Can not create a uri with a null path.");
    }
    mUri = createURI(scheme, authority, path);
  }

  /**
   * Resolve a child TachyonURI against a parent TachyonURI.
   *
   * @param parent the parent
   * @param child the child
   */
  public TachyonURI(TachyonURI parent, TachyonURI child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    URI parentUri = parent.mUri;
    String parentPath = parentUri.getPath();
    if (!parentPath.endsWith(SEPARATOR) && parentPath.length() > 0) {
      parentPath += SEPARATOR;
    }
    try {
      parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentPath, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    URI resolved = parentUri.resolve(child.mUri);
    mUri = createURI(resolved.getScheme(), resolved.getAuthority(), resolved.getPath());
  }

  @Override
  public int compareTo(TachyonURI other) {
    return mUri.compareTo(other.mUri);
  }

  /**
   * Create the internal URI. Called by all constructors.
   *
   * @param scheme the scheme of the path. e.g. tachyon, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   * @throws IllegalArgumentException when an illegal argument is encountered
   */
  private URI createURI(String scheme, String authority, String path)
      throws IllegalArgumentException {
    try {
      return new URI(scheme, authority, normalizePath(path), null, null).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TachyonURI)) {
      return false;
    }
    TachyonURI that = (TachyonURI) o;
    return mUri.equals(that.mUri);
  }

  /**
   * Gets the authority of the TachyonURI.
   *
   * @return the authority, null if it does not have one
   */
  public String getAuthority() {
    return mUri.getAuthority();
  }

  /**
   * Return the number of elements of the path component of the TachyonURI.
   *
   * <pre>
   * /                                  = 0
   * /a                                 = 1
   * /a/b/c.txt                         = 3
   * /a/b/                              = 3
   * a/b                                = 2
   * a\b                                = 2
   * tachyon://localhost:1998/          = 0
   * tachyon://localhost:1998/a         = 1
   * tachyon://localhost:1998/a/b.txt   = 2
   * C:\a                               = 1
   * C:                                 = 0
   * </pre>
   *
   * @return the depth
   */
  public int getDepth() {
    String path = mUri.getPath();
    if (path.isEmpty()) {
      return 0;
    }
    if (hasWindowsDrive(path, true)) {
      path = path.substring(3);
    }

    int depth = 0;

    int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
    while (slash != -1) {
      depth ++;
      slash = path.indexOf(SEPARATOR, slash + 1);
    }
    return depth;
  }

  /**
   * Get the first n components of the TachyonURI path. There is no trailing separator as the path
   * will be normalized by normalizePath().
   *
   * <pre>
   * /a/b/c, 0              = /
   * /a/b/c, 1              = /a
   * /a/b/c, 2              = /a/b
   * /a/b/c, 3              = /a/b/c
   * /a/b/c, 4              = null
   * </pre>
   *
   * @param n identifies the number of path components to get
   * @return the first n path components, null if the path has less than n components
   */
  public String getLeadingPath(int n) {
    String path = mUri.getPath();
    if (n == 0 && path.indexOf(TachyonURI.SEPARATOR) == 0) { // the special case
      return TachyonURI.SEPARATOR;
    }
    int depth = getDepth();
    if (depth < n) {
      return null;
    } else if (depth == n) {
      return path;
    } else {
      String[] comp = path.split(SEPARATOR);
      return StringUtils.join(Arrays.asList(comp).subList(0, n + 1), SEPARATOR);
    }
  }

  /**
   * Whether or not the TachyonURI contains wildcard(s).
   *
   * @return {@code boolean} that indicates whether the TachyonURI contains wildcard(s)
   */
  public boolean containsWildcard() {
    return mUri.getPath().contains(WILDCARD);
  }

  /**
   * Gets the host of the TachyonURI.
   *
   * @return the host, null if it does not have one
   */
  public String getHost() {
    return mUri.getHost();
  }

  /**
   * Get the final component of the TachyonURI.
   *
   * @return the final component of the TachyonURI
   */
  public String getName() {
    String path = mUri.getPath();
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Get the parent of this TachyonURI or null if at root.
   *
   * @return the parent of this TachyonURI or null if at root
   */
  public TachyonURI getParent() {
    String path = mUri.getPath();
    int lastSlash = path.lastIndexOf('/');
    int start = hasWindowsDrive(path, true) ? 3 : 0;
    if ((path.length() == start) || // empty path
        (lastSlash == start && path.length() == start + 1)) { // at root
      return null;
    }
    String parent;
    if (lastSlash == -1) {
      parent = CUR_DIR;
    } else {
      int end = hasWindowsDrive(path, true) ? 3 : 0;
      parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
    }
    return new TachyonURI(mUri.getScheme(), mUri.getAuthority(), parent);
  }

  /**
   * Gets the path component of the TachyonURI.
   *
   * @return the path
   */
  public String getPath() {
    return mUri.getPath();
  }

  /**
   * Gets the port of the TachyonURI.
   *
   * @return the port, -1 if it does not have one
   */
  public int getPort() {
    return mUri.getPort();
  }

  /**
   * Get the scheme of the TachyonURI.
   *
   * @return the scheme, null if there is no scheme
   */
  public String getScheme() {
    return mUri.getScheme();
  }

  /**
   * Tells if the TachyonURI has authority or not.
   *
   * @return true if it has, false otherwise
   */
  public boolean hasAuthority() {
    return mUri.getAuthority() != null;
  }

  @Override
  public int hashCode() {
    return mUri.hashCode();
  }

  /**
   * Tells if this TachyonURI has scheme or not.
   *
   * @return true if it has, false otherwise
   */
  public boolean hasScheme() {
    return mUri.getScheme() != null;
  }

  /**
   * Check if the path is a windows path. This should be platform independent.
   *
   * @param path the path to check
   * @param slashed if the path starts with a slash
   * @return true if it is a windows path, false otherwise
   */
  public static boolean hasWindowsDrive(String path, boolean slashed) {
    int start = slashed ? 1 : 0;
    return path.length() >= start + 2
        && (!slashed || path.charAt(0) == '/')
        && path.charAt(start + 1) == ':'
        && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a'
        && path.charAt(start) <= 'z'));
  }

  /**
   * Tells whether or not the TachyonURI is absolute.
   *
   * <p>
   * A TachyonURI is absolute if, and only if, it has a scheme component.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this TachyonURI is absolute
   */
  public boolean isAbsolute() {
    return mUri.isAbsolute();
  }

  /**
   * Tells whether or not the path component of the TachyonURI is absolute.
   *
   * <p>
   * A path is absolute if, and only if, it starts with root.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, the TachyonURI's path component is absolute
   */
  public boolean isPathAbsolute() {
    int start = hasWindowsDrive(mUri.getPath(), true) ? 3 : 0;
    return mUri.getPath().startsWith(SEPARATOR, start);
  }

  /**
   * Tells whether or not the TachyonURI is root.
   *
   * <p>
   * A URI is root if its path equals to "/"
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this URI is root
   */
  public boolean isRoot() {
    return mUri.getPath().equals(SEPARATOR)
        || (mUri.getPath().isEmpty() && mUri.getAuthority() != null);
  }

  /**
   * Append additional path elements to the end of a TachyonURI.
   *
   * @param suffix the suffix to add
   * @return the new TachyonURI
   */
  public TachyonURI join(String suffix) {
    return new TachyonURI(getScheme(), getAuthority(), getPath() + TachyonURI.SEPARATOR + suffix);
  }

  /**
   * Append additional path elements to the end of a TachyonURI.
   *
   * @param suffix the suffix to add
   * @return the new TachyonURI
   */
  public TachyonURI join(TachyonURI suffix) {
    return join(suffix.toString());
  }

  /**
   * Normalize the path component of the TachyonURI, by replacing all "//" and "\\" with "/", and
   * trimming trailing slash from non-root path (ignoring windows drive).
   *
   * @param path the path to normalize
   * @return the normalized path
   */
  private String normalizePath(String path) {
    while (path.contains("\\")) {
      path = path.replace("\\", "/");
    }
    while (path.contains("//")) {
      path = path.replace("//", "/");
    }

    int minLength = hasWindowsDrive(path, true) ? 4 : 1;
    while (path.length() > minLength && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  /**
   * Illegal characters unescaped in the string, for glob processing, etc.
   *
   * @return the String representation of the URI
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (mUri.getScheme() != null) {
      sb.append(mUri.getScheme());
      sb.append("://");
    }
    if (mUri.getAuthority() != null) {
      if (mUri.getScheme() == null) {
        sb.append("//");
      }
      sb.append(mUri.getAuthority());
    }
    if (mUri.getPath() != null) {
      String path = mUri.getPath();
      if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) && // has windows drive
          mUri.getScheme() == null && // but no scheme
          mUri.getAuthority() == null) { // or authority
        path = path.substring(1); // remove slash before drive
      }
      sb.append(path);
    }
    return sb.toString();
  }
}
