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

package alluxio;

import alluxio.util.URIUtils;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a URI in the Alluxio system. This {@link AlluxioURI} can represent
 * resources in the Alluxio namespace, as well as UFS namespaces.
 *
 * It uses a {@link URI} internally. URI requires that String is escaped, {@link AlluxioURI} does
 * not.
 *
 * Does not support fragment in the URI.
 */
@ThreadSafe
public final class AlluxioURI implements Comparable<AlluxioURI>, Serializable {
  private static final long serialVersionUID = -1207227692436086387L;
  public static final String SEPARATOR = "/";
  public static final String CUR_DIR = ".";
  public static final String WILDCARD = "*";

  public static final AlluxioURI EMPTY_URI = new AlluxioURI("");

  /** A {@link URI} is used to hold the URI components. */
  private final URI mUri;

  /**
   * Constructs an {@link AlluxioURI} from a String. Path strings are URIs, but with unescaped
   * elements and some additional normalization.
   *
   * @param pathStr path to construct the {@link AlluxioURI} from
   */
  public AlluxioURI(String pathStr) {
    mUri = URI.Factory.create(pathStr);
  }

  /**
   * Constructs an {@link AlluxioURI} from components.
   *
   * @param scheme the scheme of the path. e.g. alluxio, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   */
  public AlluxioURI(String scheme, String authority, String path) {
    mUri = URI.Factory.create(scheme, authority, path, null);
  }

  /**
   * Constructs an {@link AlluxioURI} from components.
   *
   * @param scheme the scheme of the path. e.g. alluxio, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   * @param queryMap the (nullable) map of key/value pairs for the query component of the URI
   */
  public AlluxioURI(String scheme, String authority, String path, Map<String, String> queryMap) {
    mUri = URI.Factory.create(scheme, authority, path, URIUtils.generateQueryString(queryMap));
  }

  /**
   * Resolves a child {@link AlluxioURI} against a parent {@link AlluxioURI}.
   *
   * @param parent the parent
   * @param child the child
   */
  public AlluxioURI(AlluxioURI parent, AlluxioURI child) {
    mUri = URI.Factory.create(parent.mUri, child.mUri);
  }

  /**
   * Constructs an {@link AlluxioURI} from components.
   *
   * @param scheme the scheme of the path. e.g. alluxio, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   * @param query the query component of the URI
   */
  private AlluxioURI(String scheme, String authority, String path, String query) {
    mUri = URI.Factory.create(scheme, authority, path, query);
  }

  @Override
  public int compareTo(AlluxioURI other) {
    return mUri.compareTo(other.mUri);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AlluxioURI)) {
      return false;
    }
    AlluxioURI that = (AlluxioURI) o;
    return mUri.equals(that.mUri);
  }

  /**
   * Gets the authority of the {@link AlluxioURI}.
   *
   * @return the authority, null if it does not have one
   */
  public String getAuthority() {
    return mUri.getAuthority();
  }

  /**
   * Returns the number of elements of the path component of the {@link AlluxioURI}.
   *
   * <pre>
   * /                                  = 0
   * /a                                 = 1
   * /a/b/c.txt                         = 3
   * /a/b/                              = 3
   * a/b                                = 2
   * a\b                                = 2
   * alluxio://localhost:1998/          = 0
   * alluxio://localhost:1998/a         = 1
   * alluxio://localhost:1998/a/b.txt   = 2
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
      depth++;
      slash = path.indexOf(SEPARATOR, slash + 1);
    }
    return depth;
  }

  /**
   * Gets the first n components of the {@link AlluxioURI} path. There is no trailing separator as
   * the path will be normalized by normalizePath().
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
    if (n == 0 && path.indexOf(AlluxioURI.SEPARATOR) == 0) { // the special case
      return AlluxioURI.SEPARATOR;
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
   * Whether or not the {@link AlluxioURI} contains wildcard(s).
   *
   * @return {@code boolean} that indicates whether the {@link AlluxioURI} contains wildcard(s)
   */
  public boolean containsWildcard() {
    return mUri.getPath().contains(WILDCARD);
  }

  /**
   * Gets the host of the {@link AlluxioURI}.
   *
   * @return the host, null if it does not have one
   */
  public String getHost() {
    return mUri.getHost();
  }

  /**
   * Gets the final component of the {@link AlluxioURI}.
   *
   * @return the final component of the {@link AlluxioURI}
   */
  public String getName() {
    String path = mUri.getPath();
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Get the parent of this {@link AlluxioURI} or null if at root.
   *
   * @return the parent of this {@link AlluxioURI} or null if at root
   */
  public AlluxioURI getParent() {
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
    return new AlluxioURI(mUri.getScheme(), mUri.getAuthority(), parent, mUri.getQuery());
  }

  /**
   * Gets the path component of the {@link AlluxioURI}.
   *
   * @return the path
   */
  public String getPath() {
    return mUri.getPath();
  }

  /**
   * Gets the port of the {@link AlluxioURI}.
   *
   * @return the port, -1 if it does not have one
   */
  public int getPort() {
    return mUri.getPort();
  }

  /**
   * Gets the map of query parameters.
   *
   * @return the map of query parameters
   */
  public Map<String, String> getQueryMap() {
    return URIUtils.parseQueryString(mUri.getQuery());
  }

  /**
   * Gets the scheme of the {@link AlluxioURI}.
   *
   * @return the scheme, null if there is no scheme
   */
  public String getScheme() {
    return mUri.getScheme();
  }

  /**
   * Tells if the {@link AlluxioURI} has authority or not.
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
   * Tells if this {@link AlluxioURI} has scheme or not.
   *
   * @return true if it has, false otherwise
   */
  public boolean hasScheme() {
    return mUri.getScheme() != null;
  }

  /**
   * Checks if the path is a windows path. This should be platform independent.
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
   * Tells whether or not the {@link AlluxioURI} is absolute.
   *
   * <p>
   * An {@link AlluxioURI} is absolute if, and only if, it has a scheme component.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this {@link AlluxioURI} is absolute
   */
  public boolean isAbsolute() {
    return mUri.isAbsolute();
  }

  /**
   * Tells whether or not the path component of the {@link AlluxioURI} is absolute.
   *
   * <p>
   * A path is absolute if, and only if, it starts with root.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, the {@link AlluxioURI}'s path component is absolute
   */
  public boolean isPathAbsolute() {
    int start = hasWindowsDrive(mUri.getPath(), true) ? 3 : 0;
    return mUri.getPath().startsWith(SEPARATOR, start);
  }

  /**
   * Tells whether or not the {@link AlluxioURI} is root.
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
   * Append additional path elements to the end of an {@link AlluxioURI}.
   *
   * @param suffix the suffix to add
   * @return the new {@link AlluxioURI}
   */
  public AlluxioURI join(String suffix) {
    return new AlluxioURI(getScheme(), getAuthority(), getPath() + AlluxioURI.SEPARATOR + suffix,
        mUri.getQuery());
  }

  /**
   * Append additional path elements to the end of an {@link AlluxioURI}.
   *
   * @param suffix the suffix to add
   * @return the new {@link AlluxioURI}
   */
  public AlluxioURI join(AlluxioURI suffix) {
    return join(suffix.toString());
  }

  /**
   * Normalize the path component of the {@link AlluxioURI}, by replacing all "//" and "\\" with
   * "/", and trimming trailing slash from non-root path (ignoring windows drive).
   *
   * @param path the path to normalize
   * @return the normalized path
   */
  public static String normalizePath(String path) {
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
   * @return the String representation of the {@link AlluxioURI}
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
    if (mUri.getQuery() != null) {
      sb.append("?");
      sb.append(mUri.getQuery());
    }
    return sb.toString();
  }
}
