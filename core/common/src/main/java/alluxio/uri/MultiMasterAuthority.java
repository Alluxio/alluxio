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
import java.util.HashSet;
import java.util.Set;

/**
 * A multi-master authority implementation.
 */
public class MultiMasterAuthority implements Authority {
  private static final long serialVersionUID = 2580736424809131651L;

  /**
   * The master addresses transform from the original authority string.
   * Semicolons and plus signs in authority are replaced by commas to match our
   * internal property format.
   */
  private final String mMasterAddresses;

  /**
   * @param masterAddresses the multi master addresses
   */
  public MultiMasterAuthority(String masterAddresses) {
    mMasterAddresses = masterAddresses;
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

    Set<String> firstAuthority = new HashSet<>(Arrays.asList(toString().split(",")));
    Set<String> secondAuthority = new HashSet<>(Arrays.asList(that.toString().split(",")));

    return firstAuthority.equals(secondAuthority);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mMasterAddresses);
  }

  @Override
  public String toString() {
    return mMasterAddresses;
  }
}
