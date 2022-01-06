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

import java.util.Arrays;

/**
 * {@link ZookeeperAuthority} supports authority containing Zookeeper addresses.
 */
public final class ZookeeperAuthority implements Authority {
  private static final long serialVersionUID = -3549197285125519688L;

  private final String[] mZkAddress;

  /**
   * @param zkAddress the zookeeper address inside the uri
   */
  public ZookeeperAuthority(String zkAddress) {
    mZkAddress = zkAddress.split(",");
    Arrays.sort(mZkAddress);
  }

  /**
   * @return the Zookeeper address in this authority
   */
  public String getZookeeperAddress() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < mZkAddress.length; i++)
    {
      sb.append(mZkAddress[i]);
      if (i != mZkAddress.length - 1) {
        sb.append(',');
      }
    }

    return sb.toString();
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

    return getZookeeperAddress().equals(that.getZookeeperAddress());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getZookeeperAddress());
  }

  @Override
  public String toString() {
    return "zk@" + getZookeeperAddress();
  }
}
