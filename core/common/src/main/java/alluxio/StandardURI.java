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
  protected final java.net.URI mUri;

  /**
   * @param scheme the scheme string of the URI
   * @param authority the authority string of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public StandardURI(String scheme, String authority, String path, String query) {
    try {
      mUri = new java.net.URI(scheme, authority, AlluxioURI.normalizePath(path), query, null)
          .normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public String getAuthority() {
    return mUri.getAuthority();
  }

  @Override
  public String getHost() {
    return mUri.getHost();
  }

  @Override
  public String getPath() {
    return mUri.getPath();
  }

  @Override
  public String getQuery() {
    return mUri.getQuery();
  }

  @Override
  public int getPort() {
    return mUri.getPort();
  }

  @Override
  public String getScheme() {
    return mUri.getScheme();
  }

  @Override
  public boolean isAbsolute() {
    return getScheme() != null;
  }

  @Override
  public java.net.URI getBaseURI() {
    return mUri;
  }

  @Override
  public int compareTo(URI other) {
    // Compare full schemes first.
    int compare = compareScheme(other);
    if (compare != 0) {
      return compare;
    }
    // schemes are equal, so use java.net.URI compare.
    return mUri.compareTo(other.getBaseURI());
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
    return mUri.equals(that.getBaseURI());
  }

  @Override
  public int hashCode() {
    return Objects.hash(mUri, getScheme());
  }
}
