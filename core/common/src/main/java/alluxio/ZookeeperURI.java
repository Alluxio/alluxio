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

import alluxio.util.URIUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link ZookeeperURI} supports Alluxio on Zookeeper URI.
 */
@ThreadSafe
public final class ZookeeperURI extends StandardURI {
  private static final long serialVersionUID = -3549197285125519688L;

  private final String mZookeeperAddress;

  /**
   * @param scheme the scheme string of the URI
   * @param authority the authority string of the URI
   * @param path the path component of the URI
   * @param query the query component of the URI
   */
  public ZookeeperURI(String scheme, String authority, String path,
      String query) {
    super(scheme, authority, path, query);
    mZookeeperAddress = getZookeeperAddress(authority);
  }

  /**
   * Constructs a new URI from a base URI, but with a new path component.
   *
   * @param baseUri the base uri
   * @param newPath the new path component
   */
  protected ZookeeperURI(URI baseUri, String newPath) {
    super(baseUri, newPath);
    mZookeeperAddress = baseUri.getAuthority();
  }

  @Override
  public URI createNewPath(String newPath, boolean checkNormalization) {
    if (checkNormalization && URIUtils.needsNormalization(newPath)) {
      return new ZookeeperURI(mScheme, mAuthority, newPath, mQuery);
    }
    return new ZookeeperURI(this, newPath);
  }

  @Override
  public String getAuthority() {
    return mZookeeperAddress;
  }

  /**
   * @param authority the authority of URI
   * @return the Zookeeper addresses of the authority
   */
  private String getZookeeperAddress(String authority) {
    Pattern pattern = Pattern.compile("^zk@(.*)");
    Matcher matcher = pattern.matcher(authority);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      throw new IllegalArgumentException("Alluxio on Zookeeper URI should be of format"
          + "alluxio://zk@host:port/path");
    }
  }
}
