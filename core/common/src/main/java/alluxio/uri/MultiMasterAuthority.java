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
 * A multi-master authority implementation.
 */
public class MultiMasterAuthority implements Authority {
  private static final long serialVersionUID = 2580736424809131651L;

  /** The original authority string. */
  private final String mAuthority;
  /**
   * The master addresses transform from the original authority string.
   * Semicolons in authority are replaced by commas to match our
   * internal property format.
   */
  private final String mMasterAddresses;

  /**
   * @param authority the authority string of the URI
   */
  public MultiMasterAuthority(String authority) {
    Preconditions.checkArgument(authority != null && authority.length() != 0,
        "authority should not be null or empty string");
    mAuthority = authority;
    mMasterAddresses = authority.replaceAll("[;+]", ",");
  }

  /**
   * @return the Alluxio master addresses from the authority
   */
  public String getMasterAddresses() {
    return mMasterAddresses;
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
    if (!(o instanceof MultiMasterAuthority)) {
      return false;
    }
    MultiMasterAuthority that = (MultiMasterAuthority) o;
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
