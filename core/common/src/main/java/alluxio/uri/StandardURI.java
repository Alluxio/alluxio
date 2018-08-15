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

package alluxio.uri;

import alluxio.AlluxioURI;
import alluxio.util.URIUtils;

import java.net.URISyntaxException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A standard URI implementation.
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
  protected final Authority mAuthority;
  protected final String mPath;
  protected final String mQuery;

  protected int mHashCode;

  /**
   * @param scheme the scheme string of the URI
   * @param authority the Authority of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public StandardURI(String scheme, Authority authority, String path, String query) {
    try {
      // Use java.net.URI to parse the URI components.
      java.net.URI uri;
      if (AlluxioURI.CUR_DIR.equals(path)) {
        uri = new java.net.URI(scheme,
            authority.toString().equals("") ? null : authority.toString(),
            AlluxioURI.normalizePath(path), query, null);
      } else {
        uri = new java.net.URI(scheme,
            authority.toString().equals("") ? null : authority.toString(),
            AlluxioURI.normalizePath(path), query, null).normalize();
      }
      mScheme = uri.getScheme();
      mSchemeSpecificPart = uri.getSchemeSpecificPart();
      mAuthority = authority;
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
    mPath = AlluxioURI.normalizePath(newPath);
    mQuery = baseUri.getQuery();
  }

  @Override
  public URI createNewPath(String newPath, boolean checkNormalization) {
    if (checkNormalization && URIUtils.needsNormalization(newPath)) {
      return new StandardURI(mScheme, mAuthority, newPath, mQuery);
    }
    return new StandardURI(this, newPath);
  }

  @Override
  public Authority getAuthority() {
    return mAuthority;
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
  public int compareTo(URI other) {
    // Compare full schemes first.
    int compare = compareScheme(other);
    if (compare != 0) {
      return compare;
    }

    // schemes are equal.
    if (mPath == null) {
      if (other.getPath() == null) {
        if ((compare = URIUtils.compare(mSchemeSpecificPart, other.getSchemeSpecificPart()))
            != 0) {
          return compare;
        }
        return 0;
      }
      return 1;
    } else if (other.getPath() == null) {
      return -1;
    }

    if ((compare = mAuthority.compareTo(other.getAuthority())) != 0) {
      return compare;
    }

    if ((compare = URIUtils.compare(mPath, other.getPath())) != 0) {
      return compare;
    }
    if ((compare = URIUtils.compare(mQuery, other.getQuery())) != 0) {
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

    if ((this.mPath == null && that.mPath != null) || (this.mPath != null && that.mPath == null)) {
      return false;
    }

    if (this.mPath == null) {
      return URIUtils.equals(this.mSchemeSpecificPart, that.mSchemeSpecificPart);
    }

    return URIUtils.equals(this.mPath, that.mPath)
        && URIUtils.equals(this.mQuery, that.mQuery)
        && this.mAuthority.equals(that.mAuthority);
  }

  @Override
  public int hashCode() {
    if (mHashCode != 0) {
      return mHashCode;
    }

    int hashCode = URIUtils.hashIgnoreCase(0, getScheme());
    if (mPath == null) {
      hashCode = URIUtils.hash(hashCode, mSchemeSpecificPart);
    } else {
      hashCode = URIUtils.hash(hashCode, mPath);
      hashCode = URIUtils.hash(hashCode, mQuery);
      hashCode = URIUtils.hash(hashCode, mAuthority.toString());
    }
    mHashCode = hashCode;
    return hashCode;
  }
}
