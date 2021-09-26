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
 * {@link NameServiceAuthority} supports multiple alluxio cluster for the scheme.
 */
public class NameServiceAuthority implements Authority {
  private static final long serialVersionUID = -6868960425092067073L;
  private final String mNamservice;

  public NameServiceAuthority(String nameservice) {
    mNamservice = nameservice;
  }

  public String getmNamservice() {
    return mNamservice;
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
    if (!(o instanceof NameServiceAuthority)) {
      return false;
    }
    NameServiceAuthority that = (NameServiceAuthority) o;
    return toString().equals(that.toString());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNamservice);
  }

  @Override
  public String toString() {
    return mNamservice;
  }
}
