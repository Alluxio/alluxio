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

import alluxio.collections.Pair;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * It uses a hierarchical URI internally. URI requires that String is escaped, {@link AlluxioURI}
 * does not.
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

  /**
   * A hierarchical URI. java.net.URI is used to hold the URI components as well as to reuse URI
   * functionality.
   */
  private final URI mUri;

  /**
   * java.net.URI does not handle a sub-component in the scheme. If the scheme has a sub-component,
   * this prefix holds the first components, while the java.net.URI will only consider the last
   * component. For example, the uri 'scheme:part1:part2://localhost:1234/' has multiple
   * components in the scheme, so this variable will hold 'scheme:part1', while java.net.URI will
   * handle the URI starting from 'part2'.
   */
  private final String mSchemePrefix;

  /** A map representation of the query component of the URI. It may be empty. */
  private final Map<String, String> mQueryMap;

  /**
   * Constructs an {@link AlluxioURI} from a String. Path strings are URIs, but with unescaped
   * elements and some additional normalization.
   *
   * @param pathStr path to construct the {@link AlluxioURI} from
   */
  public AlluxioURI(String pathStr) {
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
    String query = null;

    int start = 0;

    // parse uri scheme, if any
    int colon = pathStr.indexOf(':');
    int slash = pathStr.indexOf('/');
    if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
      if (slash != -1) {
        // There is a slash. The scheme may have multiple parts, so the scheme is everything before
        // the slash.
        start = slash;

        // Ignore any trailing colons from the scheme.
        while (slash > 0 && pathStr.charAt(slash - 1) == ':') {
          slash--;
        }
        scheme = pathStr.substring(0, slash);
      } else {
        // There is no slash. The scheme is the component before the first colon.
        scheme = pathStr.substring(0, colon);
        start = colon + 1;
      }
    }

    // Handle schemes with two components.
    Pair<String, String> schemeComponents = getSchemeComponents(scheme);
    mSchemePrefix = schemeComponents.getFirst();
    scheme = schemeComponents.getSecond();

    // parse uri authority, if any
    if (pathStr.startsWith("//", start) && (pathStr.length() - start > 2)) { // has authority
      int nextSlash = pathStr.indexOf('/', start + 2);
      int authEnd = nextSlash > 0 ? nextSlash : pathStr.length();
      authority = pathStr.substring(start + 2, authEnd);
      start = authEnd;
    }

    // uri path is the rest of the string -- fragment not supported
    String path = pathStr.substring(start, pathStr.length());

    // Parse the query part.
    int question = path.indexOf('?');
    if (question != -1) {
      // There is a query.
      query = path.substring(question + 1);
      path = path.substring(0, question);
    }
    mQueryMap = parseQueryString(query);

    mUri = createURI(scheme, authority, path, query);
  }

  /**
   * Constructs an {@link AlluxioURI} from components.
   *
   * @param scheme the scheme of the path. e.g. alluxio, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   */
  public AlluxioURI(String scheme, String authority, String path) {
    this(scheme, authority, path, null);
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
    if (path == null) {
      throw new IllegalArgumentException("Can not create a uri with a null path.");
    }

    // Handle schemes with two components.
    Pair<String, String> schemeComponents = getSchemeComponents(scheme);
    mSchemePrefix = schemeComponents.getFirst();
    scheme = schemeComponents.getSecond();
    mQueryMap = queryMap;

    mUri = createURI(scheme, authority, path, generateQueryString());
  }

  /**
   * Resolves a child {@link AlluxioURI} against a parent {@link AlluxioURI}.
   *
   * @param parent the parent
   * @param child the child
   */
  public AlluxioURI(AlluxioURI parent, AlluxioURI child) {
    // Add a slash to parent's path so resolution is compatible with URI's
    URI parentUri = parent.mUri;
    String parentPath = parentUri.getPath();
    if (!parentPath.endsWith(SEPARATOR) && parentPath.length() > 0) {
      parentPath += SEPARATOR;
    }
    try {
      parentUri =
          new URI(parentUri.getScheme(), parentUri.getAuthority(), parentPath, parentUri.getQuery(),
              null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
    URI resolved = parentUri.resolve(child.mUri);

    // Determine which scheme prefix to take.
    if (child.getPath() == null || parent.getPath() == null || child.getScheme() != null) {
      // With these conditions, the resolved URI uses the child's scheme.
      mSchemePrefix = child.mSchemePrefix;
    } else {
      // Otherwise, the parent scheme is used in the resolved URI.
      mSchemePrefix = parent.mSchemePrefix;
    }

    mQueryMap = parseQueryString(resolved.getQuery());

    mUri = createURI(resolved.getScheme(), resolved.getAuthority(), resolved.getPath(),
        resolved.getQuery());
  }

  /**
   * Compares the full schemes of this URI and a given URI.
   *
   * @param other the other {@link AlluxioURI} to compare the scheme
   * @return a negative integer, zero, or a positive integer if this full scheme is respectively
   *         less than, equal to, or greater than the full scheme of the other URI.
   */
  private int compareFullScheme(AlluxioURI other) {
    // Both of these full schemes may be null.
    String fullScheme = getFullScheme(mUri.getScheme());
    String otherFullScheme = other.getFullScheme(other.mUri.getScheme());

    if (fullScheme == null && otherFullScheme == null) {
      return 0;
    }
    if (fullScheme != null) {
      if (otherFullScheme != null) {
        return fullScheme.compareToIgnoreCase(otherFullScheme);
      }
      // not null is greater than 'null'.
      return 1;
    }
    // 'null' is less than not null.
    return -1;
  }

  /**
   * Returns a {@link Pair} of components of the given scheme. A given scheme may have have two
   * components if it has the ':' character to specify a sub-protocol of the scheme. If the
   * scheme does not have multiple components, the first component will be the empty string, and
   * the second component will be the given scheme. If the given scheme is null, both components
   * in the {@link Pair} will be null.
   *
   * @param scheme the scheme string
   * @return a {@link Pair} with the scheme components
   */
  private Pair<String, String> getSchemeComponents(String scheme) {
    if (scheme == null) {
      return new Pair<>(null, null);
    }
    int colon = scheme.lastIndexOf(':');
    if (colon == -1) {
      return new Pair<>("", scheme);
    }
    return new Pair<>(scheme.substring(0, colon), scheme.substring(colon + 1));
  }

  /**
   * @param uriScheme the given scheme, that comes from a {@link URI} instance
   * @return the full scheme which combines the mSchemePrefix with the given scheme
   */
  private String getFullScheme(String uriScheme) {
    if (uriScheme == null) {
      return null;
    }
    if (mSchemePrefix.length() > 0) {
      return mSchemePrefix + ":" + uriScheme;
    }
    return uriScheme;
  }

  /**
   * @return the query string, generated from {@link #mQueryMap}
   */
  private String generateQueryString() {
    if (mQueryMap == null || mQueryMap.isEmpty()) {
      return null;
    }
    ArrayList<String> pairs = new ArrayList<>(mQueryMap.size());
    try {
      for (Map.Entry<String, String> entry : mQueryMap.entrySet()) {
        pairs.add(URLEncoder.encode(entry.getKey(), "UTF-8") + "=" + URLEncoder
            .encode(entry.getValue(), "UTF-8"));
      }
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    Joiner joiner = Joiner.on('&');
    return joiner.join(pairs);
  }

  /**
   * Parses the given query string, and returns a map of the query parameters.
   *
   * @param query the query string to parse
   * @return the map of query keys and values
   */
  private Map<String, String> parseQueryString(String query) {
    Map<String, String> queryMap = new HashMap<>();
    if (query == null || query.isEmpty()) {
      return queryMap;
    }
    // The query string should escape '&'.
    String[] entries = query.split("&");

    try {
      for (String entry : entries) {
        // There should be at most 2 parts, since key and value both should escape '='.
        String[] parts = entry.split("=");
        if (parts.length == 0) {
          // Skip this empty entry.
        } else if (parts.length == 1) {
          // There is no value part. Just use empty string as the value.
          String key = URLDecoder.decode(parts[0], "UTF-8");
          queryMap.put(key, "");
        } else {
          // Save the key and value.
          String key = URLDecoder.decode(parts[0], "UTF-8");
          String value = URLDecoder.decode(parts[1], "UTF-8");
          queryMap.put(key, value);
        }
      }
    } catch (UnsupportedEncodingException e) {
      // This is unexpected.
      throw new RuntimeException(e);
    }
    return queryMap;
  }

  @Override
  public int compareTo(AlluxioURI other) {
    // Compare full schemes first.
    int compare = compareFullScheme(other);
    if (compare != 0) {
      return compare;
    }
    // Full schemes are equal, so use java.net.URI compare.
    return mUri.compareTo(other.mUri);
  }

  /**
   * Creates the internal URI. Called by all constructors.
   *
   * @param scheme the scheme of the path. e.g. alluxio, hdfs, s3, file, null, etc
   * @param authority the authority of the path. e.g. localhost:19998, 203.1.2.5:8080
   * @param path the path component of the URI. e.g. /abc/c.txt, /a b/c/c.txt
   * @param query the query component of the URI. e.g. "a=b", "a=b,c=d,e=f"
   * @throws IllegalArgumentException when an illegal argument is encountered
   */
  private URI createURI(String scheme, String authority, String path, String query)
      throws IllegalArgumentException {
    try {
      return new URI(scheme, authority, normalizePath(path), query, null).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
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
    if (compareFullScheme(that) != 0) {
      return false;
    }
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
    return new AlluxioURI(getFullScheme(mUri.getScheme()), mUri.getAuthority(), parent, mQueryMap);
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
    if (mQueryMap == null || mQueryMap.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(mQueryMap);
  }

  /**
   * Gets the scheme of the {@link AlluxioURI}.
   *
   * @return the scheme, null if there is no scheme
   */
  public String getScheme() {
    return getFullScheme(mUri.getScheme());
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
    return Objects.hash(mUri, mSchemePrefix);
  }

  /**
   * Tells if this {@link AlluxioURI} has scheme or not.
   *
   * @return true if it has, false otherwise
   */
  public boolean hasScheme() {
    return getFullScheme(mUri.getScheme()) != null;
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
        mQueryMap);
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
   * @return the String representation of the {@link AlluxioURI}
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String fullScheme = getFullScheme(mUri.getScheme());
    if (fullScheme != null) {
      sb.append(fullScheme);
      sb.append("://");
    }
    if (mUri.getAuthority() != null) {
      if (fullScheme == null) {
        sb.append("//");
      }
      sb.append(mUri.getAuthority());
    }
    if (mUri.getPath() != null) {
      String path = mUri.getPath();
      if (path.indexOf('/') == 0 && hasWindowsDrive(path, true) && // has windows drive
          fullScheme == null && // but no scheme
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
