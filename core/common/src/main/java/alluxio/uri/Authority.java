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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This interface represents the authority part of a URI.
 */
public interface Authority extends Comparable<Authority>, Serializable {
  Logger LOG = LoggerFactory.getLogger(Authority.class);
  Pattern LOGICAL_MASTER_AUTH = Pattern.compile("^ebj@([a-zA-Z_\\-0-9.]+)$");
  Pattern LOGICAL_ZOOKEEPER_AUTH = Pattern.compile("^zk@([a-zA-Z_\\-0-9.]+)$");
  Pattern SINGLE_MASTER_AUTH = Pattern.compile("^([^:,;]+):(\\d+)$");
  // We allow zookeeper/multi_master authorities to be delimited by ',' ';' or '+'.
  Pattern ZOOKEEPER_AUTH = Pattern.compile("^zk@([^:,;+]+:\\d+([,;+][^:,;+]+:\\d+)*)$");
  Pattern MULTI_MASTERS_AUTH = Pattern.compile("^[^:,;+]+:\\d+([,;+][^:,;+]+:\\d+)+$");

  /**
   * Gets the Authority object from the input string.
   *
   * @param authority the string authority to transfer
   * @return an Authority object
   */
  static Authority fromString(String authority) {
    if (authority == null || authority.length() == 0) {
      return NoAuthority.INSTANCE;
    }
    Matcher matcher = ZOOKEEPER_AUTH.matcher(authority);
    if (matcher.matches()) {
      return new ZookeeperAuthority(matcher.group(1).replaceAll("[;+]", ","));
    }
    matcher = SINGLE_MASTER_AUTH.matcher(authority);
    if (matcher.matches()) {
      return new SingleMasterAuthority(matcher.group(1), Integer.parseInt(matcher.group(2)));
    }
    matcher = MULTI_MASTERS_AUTH.matcher(authority);
    if (matcher.matches()) {
      return new MultiMasterAuthority(authority.replaceAll("[;+]", ","));
    }
    matcher = LOGICAL_ZOOKEEPER_AUTH.matcher(authority);
    if (matcher.matches()) {
      return new ZookeeperLogicalAuthority(matcher.group(1));
    }
    matcher = LOGICAL_MASTER_AUTH.matcher(authority);
    if (matcher.matches()) {
      return new EmbeddedLogicalAuthority(matcher.group(1));
    }
    return new UnknownAuthority(authority);
  }

  @Override
  default int compareTo(Authority o) {
    return toString().compareTo(o.toString());
  }
}
