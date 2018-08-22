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
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This interface represents the authority part of a URI.
 */
public interface Authority extends Comparable<Authority>, Serializable {
  Logger LOG = LoggerFactory.getLogger(Authority.class);
  Pattern ZOOKEEPER_AUTH = Pattern.compile("^zk@(.*)");

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
    if (matcher.find()) {
      return new ZookeeperAuthority(authority,
          matcher.group(1).replaceAll(";", ","));
    } else {
      java.net.URI uri;
      try {
        // Use java.net.URI to parse the authority
        uri = new java.net.URI("foo", authority, "/", null, null);
      } catch (URISyntaxException e) {
        LOG.warn("Failed to parse the authority {} of the URI: {}", authority, e.getMessage());
        throw new IllegalArgumentException(e);
      }
      if (uri.getHost() != null) {
        return new SingleMasterAuthority(authority, uri.getHost(), uri.getPort());
      } else {
        return new UnknownAuthority(authority);
      }
    }
  }
}
