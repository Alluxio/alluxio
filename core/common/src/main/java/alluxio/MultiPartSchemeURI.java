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

import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link MultiPartSchemeURI} supports multiple components for the scheme.
 */
@ThreadSafe
public final class MultiPartSchemeURI extends StandardURI {
  private static final long serialVersionUID = 8172074724822918501L;

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
   * @param authority the authority string of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public MultiPartSchemeURI(String schemePrefix, String scheme, String authority, String path,
      String query) {
    super(scheme, authority, path, query);
    mFullScheme = getFullScheme(schemePrefix, mUri.getScheme());
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
  private String getFullScheme(String schemePrefix, String uriScheme) {
    if (uriScheme == null) {
      return null;
    }
    if (schemePrefix == null || schemePrefix.isEmpty()) {
      return uriScheme;
    }
    return schemePrefix + ":" + uriScheme;
  }
}
