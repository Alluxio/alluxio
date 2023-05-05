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
 * A Singleton of NoAuthority, it represents a URI without authority.
 */
public final class NoAuthority implements Authority {
  private static final long serialVersionUID = -208199254267143112L;

  public static final NoAuthority INSTANCE = new NoAuthority();

  private NoAuthority() {} // enforce singleton pattern

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
