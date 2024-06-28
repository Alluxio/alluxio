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
  private int mHashCode = 0;

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
    // rationale:
    // 1. calculate each comma-separated segment's hashcode in the same way
    //    Java calculates the hashcode of a String object.
    // 2. then XOR each segment's hashcode to produce the hashcode for this object.
    //    since XOR is commutative, the order of the segments does not matter.
    //    as long as the number of the segments and the hashcodes of the segments are equal,
    //    the resulting hashcodes are equal.

    // we might need to update the hashcode field,
    // so copy to a local variable to avoid data race
    int h = mHashCode;
    if (mLogicalName.isEmpty() || h != 0) {
      // either the string is empty, so the hashcode is 0,
      // or we have calculated and cached the hashcode
      return h;
    }
    final int prime = 31;
    int segmentHashCode = 0;
    for (int i = 0; i < mLogicalName.length(); i++) {
      segmentHashCode = prime * segmentHashCode + mLogicalName.charAt(i);
    }
    h ^= segmentHashCode;

    mHashCode = h;
    return h;
  }

  @Override
  public String toString() {
    return "zk@" + mLogicalName;
  }
}
