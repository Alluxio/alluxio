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

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This interface represents the authority part of a URI.
 */
public interface Authority extends Comparable<Authority>, Serializable {
  public static final Pattern ZOOKEEPER_AUTH = Pattern.compile("^zk@(.*)");

  /**
   * Gets the Authority object from the input string.
   *
   * @param authority the string authority to transfer
   * @return an Authority object
   */
  @Nullable
  static Authority fromString(String authority) {
    if (authority == null || authority.length() == 0) {
      return null;
    }
    Matcher matcher = ZOOKEEPER_AUTH.matcher(authority);
    if (matcher.find()) {
      return new ZookeeperAuthority(authority,
          matcher.group(1).replaceAll(";", ","));
    } else {
      return new HostAuthority(authority);
    }
  }
}
