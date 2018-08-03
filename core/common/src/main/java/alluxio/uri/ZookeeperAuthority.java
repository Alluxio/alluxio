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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link ZookeeperAuthority} supports authority containing Zookeeper addresses.
 */
public final class ZookeeperAuthority extends HostnameAuthority {
  private static final long serialVersionUID = -3549197285125519688L;
  private static final Pattern ZOOKEEPER_PATTERN = Pattern.compile("^zk@(.*)");

  private final String mZookeeperAddress;

  /**
   * @param authority the authority string of the URI
   */
  public ZookeeperAuthority(String authority) {
    super(authority);
    mZookeeperAddress = getZookeeperAddress(authority);
  }

  /**
   * @return the Zookeeper address
   */
  public String getZookeeperAddress() {
    return mZookeeperAddress;
  }

  /**
   * Gets zookeeper address from the authority.
   *
   * @param authority an authority to get zookeeper address from
   * @return the Zookeeper addresses
   */
  private String getZookeeperAddress(String authority) {
    Matcher matcher = ZOOKEEPER_PATTERN.matcher(authority);
    if (matcher.find()) {
      return matcher.group(1).replaceAll(";", ",");
    } else {
      throw new IllegalArgumentException("Alluxio URI in high availability mode "
          + "should be of format alluxio://zk@host:port/path");
    }
  }
}
