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

package alluxio.security;

import com.google.common.base.Objects;

import java.security.Principal;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents the current user, which represents the end user. This may differ from
 * the Alluxio login user, {@link User}.
 */
@ThreadSafe
public final class CurrentUser implements Principal {
  private final String mName;
  private final String mServiceName;

  /**
   * Constructs a new user with a name.
   *
   * @param name the name of the user
   */
  public CurrentUser(String name) {
    this(name, "");
  }

  /**
   * Constructs a new user with a name and a service name.
   *
   * @param name the name of the user
   * @param serviceName the name of the service
   */
  public CurrentUser(String name, String serviceName) {
    mName = name;
    mServiceName = serviceName;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public String toString() {
    return mName;
  }

  /**
   * @return the service name
   */
  public String getServiceName() {
    return mServiceName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mName, mServiceName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CurrentUser)) {
      return false;
    }
    CurrentUser other = (CurrentUser) o;
    return Objects.equal(mName, other.mName)
        && Objects.equal(mServiceName, other.mServiceName);
  }
}
