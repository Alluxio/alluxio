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

/**
 * {@link MultiPartSchemeURI} supports multiple components for the scheme.
 */
public class MultiPartSchemeURI extends StandardURI {
  /**
   * {@link java.net.URI} does not handle a sub-component in the scheme. If the scheme has a
   * sub-component, this prefix holds the first components, while the java.net.URI will only
   * consider the last component. For example, the uri 'scheme:part1:part2://localhost:1234/' has
   * multiple components in the scheme, so this variable will hold 'scheme:part1', while
   * {@link java.net.URI} will handle the URI starting from 'part2'.
   */
  private final String mSchemePrefix;

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
    mSchemePrefix = schemePrefix;
  }

  @Override
  public String getScheme() {
    String uriScheme = mUri.getScheme();
    if (uriScheme == null) {
      return null;
    }
    if (mSchemePrefix == null || mSchemePrefix.isEmpty()) {
      return uriScheme;
    }
    return mSchemePrefix + ":" + uriScheme;
  }
}
