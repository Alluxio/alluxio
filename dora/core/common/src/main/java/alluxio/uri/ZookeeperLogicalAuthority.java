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

import java.util.Objects;

/**
 * A zookeeper logical host authority implementation.
 */
public class ZookeeperLogicalAuthority implements Authority {
  private static final long serialVersionUID = 8292537475920509321L;

  /**
   * The zookeeper logical name.
   */
  private final String mLogicalName;

  /**
   * @param logicalName the zookeeper logical name
   */
  public ZookeeperLogicalAuthority(String logicalName) {
    mLogicalName = logicalName;
  }

  /**
   * @return the logical host from the authority
   */
  public String getLogicalName() {
    return mLogicalName;
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
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ZookeeperLogicalAuthority that = (ZookeeperLogicalAuthority) o;
    return Objects.equals(mLogicalName, that.mLogicalName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mLogicalName);
  }

  @Override
  public String toString() {
    return "zk@" + mLogicalName;
  }
}
