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

package alluxio;

import java.net.URISyntaxException;
import java.util.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A wrapper class around {@link java.net.URI}.
 */
@ThreadSafe
public class StandardURI implements URI {
  private static final long serialVersionUID = 3705239942914676079L;

  /**
   * A hierarchical URI. {@link java.net.URI} is used to hold the URI components as well as to
   * reuse URI functionality.
   */
  protected final String mScheme;
  protected final String mSchemeSpecificPart;
  protected final String mAuthority;
  protected final String mHost;
  protected final int mPort;
  protected final String mPath;
  protected final String mQuery;

  protected int mHashCode;


  /**
   * @param scheme the scheme string of the URI
   * @param authority the authority string of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public StandardURI(String scheme, String authority, String path, String query) {
    try {
      java.net.URI uri;
      if (AlluxioURI.CUR_DIR.equals(path)) {
        uri = new java.net.URI(scheme, authority, AlluxioURI.normalizePath(path), query, null);
      } else {
        uri = new java.net.URI(scheme, authority, AlluxioURI.normalizePath(path), query, null)
            .normalize();
      }
      mScheme = uri.getScheme();
      mSchemeSpecificPart = uri.getSchemeSpecificPart();
      mAuthority = uri.getAuthority();
      mHost = uri.getHost();
      mPort = uri.getPort();
      mPath = uri.getPath();
      mQuery = uri.getQuery();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Constructs a new URI from a base URI, but with a new path component.
   *
   * @param baseUri the base uri
   * @param newPath the new path component
   */
  protected StandardURI(URI baseUri, String newPath) {
    mScheme = baseUri.getScheme();
    mSchemeSpecificPart = baseUri.getSchemeSpecificPart();
    mAuthority = baseUri.getAuthority();
    mHost = baseUri.getHost();
    mPort = baseUri.getPort();
    mPath = AlluxioURI.normalizePath(newPath);
    mQuery = baseUri.getQuery();
  }

  @Override
  public URI createNewPath(String newPath, boolean needsNormalization) {
    if (needsNormalization && needsNormalization(newPath)) {
      return new StandardURI(mScheme, mAuthority, newPath, mQuery);
    }
    return new StandardURI(this, newPath);
  }

  @Override
  public String getAuthority() {
    return mAuthority;
  }

  @Override
  public String getHost() {
    return mHost;
  }

  @Override
  public String getPath() {
    return mPath;
  }

  @Override
  public String getQuery() {
    return mQuery;
  }

  @Override
  public int getPort() {
    return mPort;
  }

  @Override
  public String getScheme() {
    return mScheme;
  }

  @Override
  public String getSchemeSpecificPart() {
    return mSchemeSpecificPart;
  }

  @Override
  public boolean isAbsolute() {
    return getScheme() != null;
  }

  @Override
  public java.net.URI getBaseURI() {
    return null;
  }

  @Override
  public int compareTo(URI other) {
    // Compare full schemes first.
    int compare = compareScheme(other);
    if (compare != 0) {
      return compare;
    }

    // schemes are equal.
    if (mPath == null) {
      if (other.getPath() == null) {
        if ((compare = compareString(mSchemeSpecificPart, other.getSchemeSpecificPart())) != 0) {
          return compare;
        }
        return 0;
      }
      return 1;
    } else if (other.getPath() == null) {
      return -1;
    }

    if ((mHost != null) && (other.getHost() != null)) {
      // compare host-based authority
      if ((compare = mHost.compareToIgnoreCase(other.getHost())) != 0) {
        return compare;
      }
      if ((compare = mPort - other.getPort()) != 0) {
        return compare;
      }
    } else if ((compare = compareString(mAuthority, other.getAuthority())) != 0) {
      return compare;
    }

    if ((compare = compareString(mPath, other.getPath())) != 0) {
      return compare;
    }
    if ((compare = compareString(mQuery, other.getQuery())) != 0) {
      return compare;
    }
    return 0;
  }

  /**
   * Compares the schemes of this URI and a given URI.
   *
   * @param other the other {@link URI} to compare the scheme
   * @return a negative integer, zero, or a positive integer if this scheme is respectively less
   *         than, equal to, or greater than the full scheme of the other URI.
   */
  private int compareScheme(URI other) {
    String scheme = getScheme();
    String otherScheme = other.getScheme();

    if (scheme == null && otherScheme == null) {
      return 0;
    }
    if (scheme != null) {
      if (otherScheme != null) {
        return scheme.compareToIgnoreCase(otherScheme);
      }
      // not null is greater than 'null'.
      return 1;
    }
    // 'null' is less than not null.
    return -1;
  }

  /**
   * @param s1 first string (can be null)
   * @param s2 second string (can be null)
   * @return negative integer, zero, or positive integer, if the first string is less
   *         than, equal to, or greater than the second string
   */
  private static int compareString(String s1, String s2) {
    if (s1 == s2) {
      return 0;
    }
    if (s1 == null) {
      return -1;
    }
    if (s2 == null) {
      return 1;
    }
    return s1.compareTo(s2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StandardURI)) {
      return false;
    }
    StandardURI that = (StandardURI) o;
    if (compareScheme(that) != 0) {
      return false;
    }

//    return mUri.equals(that.getBaseURI());

    if ((this.mPath == null && that.mPath != null) || (this.mPath != null && that.mPath == null)) {
      return false;
    }

    if (this.mPath == null) {
      return equalsString(this.mSchemeSpecificPart, that.mSchemeSpecificPart);
    }
    if (!equalsString(this.mPath, that.mPath)) {
      return false;
    }
    if (!equalsString(this.mQuery, that.mQuery)) {
      return false;
    }

    if (this.mAuthority == that.mAuthority) {
      return true;
    }
    if (this.mHost != null) {
      // host-based authority
      if (that.mHost == null) {
        return false;
      }
      if (this.mHost.compareToIgnoreCase(that.mHost) != 0) {
        return false;
      }
      if (this.mPort != that.mPort) {
        return false;
      }
    } else if (!equalsString(this.mAuthority, that.mAuthority)) {
      return false;
    }
    return true;
  }

  private static boolean equalsString(String s1, String s2) {
    if (s1 == s2) {
      return true;
    }
    if ((s1 != null) && (s2 != null)) {
      if (s1.length() != s2.length()) {
        return false;
      }
      if (s1.indexOf('%') < 0) {
        return s1.equals(s2);
      }
      int length = s1.length();
      for (int i = 0; i < length; i++) {
        char c1 = s1.charAt(i);
        char c2 = s2.charAt(i);
        if (c1 != '%') {
          if (c1 != c2) {
            return false;
          }
          continue;
        }
        // c1 is '%'
        if (c2 != '%') {
          return false;
        }
        // check next 2 characters
        i++;
        if (toLower(s1.charAt(i)) != toLower(s2.charAt(i))) {
          return false;
        }
        i++;
        if (toLower(s1.charAt(i)) != toLower(s2.charAt(i))) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static int toLower(char c) {
    if ((c >= 'A') && (c <= 'Z')) {
      return c + ('a' - 'A');
    }
    return c;
  }

  @Override
  public int hashCode() {
    if (mHashCode != 0) {
      return mHashCode;
    }

    int hashCode = hashIgnoringCase(0, getScheme());
    if (mPath == null) {
      hashCode = hash(hashCode, mSchemeSpecificPart);
    } else {
      hashCode = hash(hashCode, mPath);
      hashCode = hash(hashCode, mQuery);
      if (mHost != null) {
        hashCode = hashIgnoringCase(hashCode, mHost);
        hashCode += 1949 * mPort;
      } else {
        hashCode = hash(hashCode, mAuthority);
      }
    }
    mHashCode = hashCode;
    return hashCode;
  }

  private static int hash(int hash, String s) {
    if (s == null) {
      return hash;
    }
    return s.indexOf('%') < 0 ? hash * 127 + s.hashCode() : normalizedHash(hash, s);
  }

  private static int normalizedHash(int hash, String s) {
    int h = 0;
    for (int index = 0; index < s.length(); index++) {
      char ch = s.charAt(index);
      h = 31 * h + ch;
      if (ch == '%') {
        // hash next 2 characters
        for (int i = index + 1; i < index + 3; i++) {
          h = 31 * h + toLower(s.charAt(i));
        }
        index += 2;
      }
    }
    return hash * 127 + h;
  }

  // US-ASCII only
  private static int hashIgnoringCase(int hash, String s) {
    if (s == null) {
      return hash;
    }
    int h = hash;
    int n = s.length();
    for (int i = 0; i < n; i++) {
      h = 31 * h + toLower(s.charAt(i));
    }
    return h;
  }

  protected static boolean needsNormalization(String path) {
    int end = path.length() - 1;    // Index of last char in path
    int p = 0;                      // Index of next char in path

    // Skip initial slashes
    while (p <= end) {
      if (path.charAt(p) != '/') {
        break;
      }
      p++;
    }
    if (p > 1) {
      return true;
    }

    // Scan segments
    while (p <= end) {

      // Looking at "." or ".." ?
      if ((path.charAt(p) == '.') && ((p == end) || ((path.charAt(p + 1) == '/') || (
          (path.charAt(p + 1) == '.') && ((p + 1 == end) || (path.charAt(p + 2) == '/')))))) {
        return true;
      }

      // Find beginning of next segment
      while (p <= end) {
        if (path.charAt(p++) != '/') {
          continue;
        }

        // Skip redundant slashes
        if (path.charAt(p) != '/') {
          break;
        }
        return true;
      }
    }

    return false;
  }

}
