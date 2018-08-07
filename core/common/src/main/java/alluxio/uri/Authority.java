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

import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * This interface represents the authority part of a URI.
 */
public interface Authority extends Comparable<Authority>, Serializable {

  /**
   * Factory for {@link Authority}.
   */
  class Factory {
    private Factory(){} //prevent initialization

    public static Authority create(String authority)  {
      Preconditions.checkNotNull(authority, "authority should not be null");
      String zkAuthorityPattern = "^zk@(.*)";
      if (authority.matches(zkAuthorityPattern)) {
        return new ZookeeperAuthority(authority);
      } else {
        return new HostnamePortAuthority(authority);
      }
    }
  }
}
