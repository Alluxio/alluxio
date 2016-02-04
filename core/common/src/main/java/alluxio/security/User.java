/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.security;

import java.security.Principal;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a user in Tachyon. It implements {@link java.security.Principal} in the
 * context of Java security frameworks.
 */
@ThreadSafe
public final class User implements Principal {
  private final String mName;

  // TODO(dong): add more attributes and methods for supporting Kerberos

  /**
   * Constructs a new user with a name.
   *
   * @param name the name of the user
   */
  public User(String name) {
    mName = name;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof User)) {
      return false;
    }
    User that = (User) o;
    return mName.equals(that.mName);
  }

  @Override
  public int hashCode() {
    return mName.hashCode();
  }

  @Override
  public String toString() {
    return mName;
  }
}
