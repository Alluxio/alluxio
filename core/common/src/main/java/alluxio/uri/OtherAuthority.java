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
import com.google.common.base.Preconditions;

/**
 * A authority that does not fall into ZookeeperAuthority,
 * SingleMasterAuthority and NoAuthority implementation.
 */
public class OtherAuthority implements Authority {
  private static final long serialVersionUID = 2580736424809131651L;

  private final String mAuthority;

  /**
   * @param authority the authority string of the URI
   */
  public OtherAuthority(String authority) {
    Preconditions.checkArgument(authority != null && authority.length() != 0,
        "authority should not be null or empty string");
    mAuthority = authority;
  }

  @Override
  public int compareTo(Authority other) {
    return mAuthority.compareTo(other.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OtherAuthority)) {
      return false;
    }
    OtherAuthority that = (OtherAuthority) o;
    return mAuthority.equals(that.mAuthority);
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
