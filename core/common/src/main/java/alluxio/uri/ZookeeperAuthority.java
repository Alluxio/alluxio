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

import com.google.common.base.Objects;

/**
 * {@link ZookeeperAuthority} supports authority containing Zookeeper addresses.
 */
public final class ZookeeperAuthority implements Authority {
  private static final long serialVersionUID = -3549197285125519688L;

  private final String mAuthority;
  private final String mZookeeperAddress;

  /**
   * @param authority the authority string of the uri
   * @param zookeeperAddress the zookeeper address inside the uri
   */
  public ZookeeperAuthority(String authority, String zookeeperAddress) {
    mAuthority = authority;
    mZookeeperAddress = zookeeperAddress;
  }

  /**
   * @return the Zookeeper address in this authority
   */
  public String getZookeeperAddress() {
    return mZookeeperAddress;
  }

  @Override
  public int compareTo(Authority other) {
    return toString().compareTo(other.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ZookeeperAuthority)) {
      return false;
    }
    ZookeeperAuthority that = (ZookeeperAuthority) o;
    return toString().equals(that.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAuthority);
  }

  @Override
  public String toString() {
    return mAuthority;
  }
}
