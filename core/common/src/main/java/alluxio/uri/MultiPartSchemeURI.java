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

import static alluxio.uri.URI.Factory.getSchemeComponents;

import alluxio.collections.Pair;
import alluxio.util.URIUtils;

import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link MultiPartSchemeURI} supports multiple components for the scheme.
 */
@ThreadSafe
public final class MultiPartSchemeURI extends StandardURI {
  private static final long serialVersionUID = 8172074724822918502L;

  /**
   * {@link java.net.URI} does not handle a sub-component in the scheme. This variable will hold
   * the full scheme with all of the components. For example, the uri
   * 'scheme:part1:part2://localhost:1234/' has multiple components in the scheme, so this
   * variable will hold 'scheme:part1:part2', because {@link java.net.URI} will only handle the
   * URI starting from 'part2'.
   */
  private final String mFullScheme;

  /**
   * @param schemePrefix the prefix of the scheme string of the URI
   * @param scheme the scheme string of the URI
   * @param authority the Authority of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public MultiPartSchemeURI(String schemePrefix, String scheme, Authority authority, String path,
      String query) {
    super(scheme, authority, path, query);
    mFullScheme = getFullScheme(schemePrefix, mScheme);
  }

  /**
   * Constructs a new URI from a base URI, but with a new path component.
   *
   * @param baseUri the base uri
   * @param fullScheme the full scheme
   * @param newPath the new path component
   */
  protected MultiPartSchemeURI(URI baseUri, String fullScheme, String newPath) {
    super(baseUri, newPath);
    mFullScheme = fullScheme;
  }

  @Override
  public URI createNewPath(String newPath, boolean checkNormalization) {
    if (checkNormalization && URIUtils.needsNormalization(newPath)) {
      // Handle schemes with two components.
      Pair<String, String> schemeComponents = getSchemeComponents(mFullScheme);
      String schemePrefix = schemeComponents.getFirst();

      return new MultiPartSchemeURI(schemePrefix, mScheme, mAuthority, newPath, mQuery);
    }
    return new MultiPartSchemeURI(this, mFullScheme, newPath);
  }

  @Override
  public String getScheme() {
    return mFullScheme;
  }

  /**
   * @param schemePrefix the prefix of the scheme
   * @param uriScheme the scheme of the URI
   * @return the combined scheme
   */
  @Nullable
  private String getFullScheme(String schemePrefix, String uriScheme) {
    if (uriScheme == null) {
      return null;
    }
    if (schemePrefix == null || schemePrefix.isEmpty()) {
      return uriScheme;
    }
    return schemePrefix + ":" + uriScheme;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }

    MultiPartSchemeURI other = (MultiPartSchemeURI) o;

    return super.equals(o)
        && mFullScheme.equals(other.mFullScheme);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), mFullScheme);
  }
}
