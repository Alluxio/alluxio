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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * It uses a hierarchical URI internally. URI requires that String is escaped, TachyonURI does not.
 *
 * Does not support fragment or query in the URI.
 */
public final class TachyonURI implements Comparable<TachyonURI> {
  public static final String SEPARATOR = "/";
  public static final String CUR_DIR = ".";

  private static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  /**
   * Create the internal URI. Called by all constructors.
   *
   * @param scheme
   *          the scheme of the path. e.g. tachyon, hdfs, s3, file, null, etc.
   * @param authority
   *          the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path
   *          the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   * @throws IllegalArgumentException
   */
  private static URI createURI(String scheme, String authority, String path)
      throws IllegalArgumentException {
    try {
      return new URI(scheme, authority, normalizePath(path), null, null).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Check if the path is a windows path.
   *
   * @param path
   *          the path to check
   * @param slashed
   *          if the path starts with a slash.
   * @return true if it is a windows path, false otherwise.
   */
  private static boolean hasWindowsDrive(String path, boolean slashed) {
    int start = slashed ? 1 : 0;
    return WINDOWS
        && path.length() >= start + 2
        && (!slashed || path.charAt(0) == '/')
        && path.charAt(start + 1) == ':'
        && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') ||
            (path.charAt(start) >= 'a' && path.charAt(start) <= 'z'));
  }

  /**
   * Normalize the path component of the TachyonURI, by replacing all "//" and "\\" with "/", and
   * trimming trailing slash from non-root path (ignoring windows drive).
   *
   * @param path
   * @return
   */
  private static String normalizePath(String path) {
    while (path.indexOf("\\") != -1) {
      path = path.replace("\\", "/");
    }
    while (path.indexOf("//") != -1) {
      path = path.replace("//", "/");
    }

    int minLength = hasWindowsDrive(path, true) ? 4 : 1;
    while (path.length() > minLength && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  // a hierarchical uri
  private final URI URI;

  /**
   * Construct a path from a String. Path strings are URIs, but with unescaped elements and some
   * additional normalization.
   */
  public TachyonURI(String pathStr) {
    if (pathStr == null) {
      throw new IllegalArgumentException("Can not create a Path from a null");
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
    if ((colon != -1) && ((slash == -1) || (colon < slash))) {     // has a scheme
      scheme = pathStr.substring(0, colon);
      start = colon + 1;
    }

    // parse uri authority, if any
    if (pathStr.startsWith("//", start) && (pathStr.length() - start > 2)
        && pathStr.substring(start).indexOf(":") != -1) {       // has authority
      int nextSlash = pathStr.indexOf('/', start + 2);
      int authEnd = nextSlash > 0 ? nextSlash : pathStr.length();
      authority = pathStr.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- query & fragment not supported
    String path = pathStr.substring(start, pathStr.length());

    URI = createURI(scheme, authority, path);
  }

  /**
   * Construct a Tachyon URI from components.
   *
   * @param scheme
   *          the scheme of the path. e.g. tachyon, hdfs, s3, file, null, etc.
   * @param authority
   *          the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path
   *          the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   */
  public TachyonURI(String scheme, String authority, String path) {
    if (path == null || path.length() == 0) {
      throw new IllegalArgumentException("Can not create a Path from a null or empty string");
    }
    URI = createURI(scheme, authority, path);
  }

  /**
   * Resolve a child TachyonURI against a parent TachyonURI.
   *
   * @param parent
   *          the parent
   * @param child
   *          the child
   */
  public TachyonURI(TachyonURI parent, TachyonURI child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    URI parentUri = parent.URI;
    String parentPath = parentUri.getPath();
    if (!parentPath.endsWith(SEPARATOR) && parentPath.length() > 0) {
      parentPath += SEPARATOR;
    }
    try {
      parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentPath, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    URI resolved = parentUri.resolve(child.URI);
    URI = createURI(resolved.getScheme(), resolved.getAuthority(), resolved.getPath());
  }

  @Override
  public int compareTo(TachyonURI other) {
    return URI.compareTo(other.URI);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TachyonURI)) {
      return false;
    }
    return URI.equals(((TachyonURI) o).URI);
  }

  /**
   * Gets the authority of this TachyonURI
   *
   * @return the authority, null if it does not have one.
   */
  public String getAuthority() {
    return URI.getAuthority();
  }

  /**
   * Return the number of elements of the path component of the TachyonURI.
   *
   * <pre>
   * /                                  -> 0
   * /a                                 -> 1
   * /a/b/c.txt                         -> 3
   * /a/b/                              -> 3
   * a/b                                -> 2
   * a\b                                -> 2
   * tachyon://localhost:1998/          -> 0
   * tachyon://localhost:1998/a         -> 1
   * tachyon://localhost:1998/a/b.txt   -> 2
   * C:\a                               -> 1
   * C:                                 -> 0
   * </pre>
   *
   * @return the depth
   */
  public int getDepth() {
    String path = URI.getPath();
    if (path.isEmpty()) {
      return 0;
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
   * Gets the host of this TachyonURI.
   *
   * @return the host, null if it does not have one.
   */
  public String getHost() {
    return URI.getHost();
  }

  /**
   * Get the final component of the TachyonURI.
   *
   * @return the final component of the TachyonURI
   */
  public String getName() {
    String path = URI.getPath();
    int slash = path.lastIndexOf(SEPARATOR);
    return path.substring(slash + 1);
  }

  /**
   * Get the parent of this TachyonURI or null if at root.
   *
   * @return the parent of this TachyonURI or null if at root.
   */
  public TachyonURI getParent() {
    String path = URI.getPath();
    int lastSlash = path.lastIndexOf('/');
    int start = hasWindowsDrive(path, true) ? 3 : 0;
    if ((path.length() == start) ||               // empty path
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
    return new TachyonURI(URI.getScheme(), URI.getAuthority(), parent);
  }

  /**
   * Gets the part component of this TachyonURI.
   *
   * @return the path.
   */
  public String getPath() {
    return URI.getPath();
  }

  /**
   * Gets the port of this TachyonURI.
   *
   * @return the port, -1 if it does not have one.
   */
  public int getPort() {
    return URI.getPort();
  }

  /**
   * Get the scheme of the Tachyon URI.
   *
   * @return the scheme, null if there is no scheme.
   */
  public String getScheme() {
    return URI.getScheme();
  }

  /**
   * Tells if this TachyonURI has authority or not.
   *
   * @return true if it has, false otherwise.
   */
  public boolean hasAuthority() {
    return URI.getAuthority() != null;
  }

  @Override
  public int hashCode() {
    return URI.hashCode();
  }

  /**
   * Tells if this TachyonURI has scheme or not.
   *
   * @return true if it has, false otherwise.
   */
  public boolean hasScheme() {
    return URI.getScheme() != null;
  }

  /**
   * Tells whether or not this URI is absolute.
   *
   * <p>
   * A URI is absolute if, and only if, it has a scheme component.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this URI is absolute
   */
  public boolean isAbsolute() {
    return URI.isAbsolute();
  }

  /**
   * Tells whether or not the path component of this TachyonURI is absolute.
   *
   * <p>
   * A path is absolute if, and only if, it starts with root.
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this TachyonURI's path component is absolute
   */
  public boolean isPathAbsolute() {
    int start = hasWindowsDrive(URI.getPath(), true) ? 3 : 0;
    return URI.getPath().startsWith(SEPARATOR, start);
  }

  /**
   * Tells whether or not this URI is root.
   *
   * <p>
   * A URI is root if its path equals to "/"
   * </p>
   *
   * @return <tt>true</tt> if, and only if, this URI is root
   */
  public boolean isRoot() {
    return URI.getPath().equals(SEPARATOR)
        || (URI.getPath().isEmpty() && URI.getAuthority() != null);
  }

  /**
   * Add a suffix to the end of the Tachyon URI.
   *
   * @param suffix
   *          the suffix to add
   * @return the new TachyonURI
   */
  public TachyonURI join(String suffix) {
    return new TachyonURI(toString() + suffix);
  }

  /**
   * Add a suffix to the end of the Tachyon URI.
   *
   * @param TachyonURI
   *          the suffix to add
   * @return the new TachyonURI
   */
  public TachyonURI join(TachyonURI suffix) {
    return new TachyonURI(toString() + suffix.toString());
  }

  /**
   * Illegal characters unescaped in the string, for glob processing, etc.
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    if (URI.getScheme() != null) {
      sb.append(URI.getScheme());
      sb.append(":");
    }
    if (URI.getAuthority() != null) {
      sb.append("//");
      sb.append(URI.getAuthority());
    }
    if (URI.getPath() != null) {
      String path = URI.getPath();
      if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) &&          // has windows drive
          URI.getScheme() == null &&              // but no scheme
          URI.getAuthority() == null) {            // or authority
        path = path.substring(1);                 // remove slash before drive
      }
      sb.append(path);
    }
    return sb.toString();
  }
}