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
 * A hostname port authority implementation.
 */
public class HostnamePortAuthority implements Authority {
  private static final long serialVersionUID = 2580736424809131651L;

  private final String mAuthority;

  /**
   * @param authority the authority string of the URI
   */
  public HostnamePortAuthority(String authority) {
    mAuthority = authority;
  }

  @Override
  public String getWholeAuthority() {
    return mAuthority;
  }

  @Override
  public int compareTo(Authority other) {
    if (mAuthority == null && other.getWholeAuthority() == null) {
      return 0;
    }
    if (mAuthority != null) {
      if (other.getWholeAuthority() != null) {
        return mAuthority.compareTo(other.getWholeAuthority());
      }
      // not null is greater than 'null'.
      return 1;
    }
    // 'null' is less than not null.
    return -1;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof HostnamePortAuthority)) {
      return false;
    }
    HostnamePortAuthority that = (HostnamePortAuthority) o;
    return compareTo(that) == 0;
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
