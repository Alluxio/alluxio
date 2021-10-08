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

package alluxio.client.file.cache.cuckoofilter;

import java.util.Objects;

/**
 * This class represents a scope.
 */
public class ScopeInfo {
  private String mScope;

  /**
   * Create a scope with specified string.
   *
   * @param scope the scope name
   */
  public ScopeInfo(String scope) {
    mScope = scope;
  }

  /**
   * @return the string information of this scope
   */
  public String getScope() {
    return mScope;
  }

  /**
   * Set the scope.
   *
   * @param scope the scope name
   */
  public void setScope(String scope) {
    mScope = scope;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ScopeInfo scopeInfo = (ScopeInfo) o;
    return Objects.equals(mScope, scopeInfo.mScope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(mScope);
  }

  @Override
  public String toString() {
    return "ScopeInfo{" + "scope='" + mScope + '\'' + '}';
  }
}
