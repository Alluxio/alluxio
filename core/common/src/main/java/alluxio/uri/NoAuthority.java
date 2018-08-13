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
 * A no authority implementation.
 */
public class NoAuthority implements Authority {
  private static final long serialVersionUID = -208199254267143112L;

  /**
   * Constructs the authority that contains nothing.
   */
  public NoAuthority() {}

  @Override
  public int compareTo(Authority other) {
    return toString().compareTo(other.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o || o instanceof NoAuthority) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode("");
  }

  @Override
  public String toString() {
    return "";
  }
}
