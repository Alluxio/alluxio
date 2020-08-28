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

package alluxio.client.quota;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class defines "Scope" of a quota. A scope is hierarchical, e.g.
 * <ul>
 *   <li>"." for global scope;</li>
 *   <li>"schema1" for an entire schema;</li>
 *   <li>"schema2.table1" for a given table;</li>
 *   <li>"schema3.table2.partition1" for a given partition</li>
 * </ul>
 */
@ThreadSafe
public class Scope {
  public static final String GLOBAL_ID = ".";
  public static final String SEPARATOR = ".";

  public static final Scope GLOBAL = new Scope(GLOBAL_ID, 1);
  private final String mId;
  private final int mLength;

  /**
   * @param id id of the scope
   * @return an instance of this scope converted from a string
   */
  public static Scope create(String id) {
    Preconditions.checkArgument(id != null && id.length() > 0,
        "scope id can not be null or empty string");
    if (GLOBAL_ID.equals(id)) {
      return GLOBAL;
    }
    return new Scope(id, id.length());
  }

  private Scope(String id, int len) {
    mId = id;
    mLength = len;
  }

  /**
   * @return parent scope
   */
  @Nullable
  public Scope parent() {
    int r = mId.lastIndexOf(SEPARATOR, mLength - 1);
    if (r < 0) {
      return GLOBAL;
    } else if (r == 0) {
      return null;
    }
    return new Scope(mId, r);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Scope scope = (Scope) o;
    if (mLength != scope.mLength) {
      return false;
    }
    if (Objects.equal(mId, scope.mId)) {
      return true;
    }
    return Objects.equal(
          mId.substring(0, mLength), scope.mId.substring(0, scope.mLength));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId.substring(0, mLength), mLength);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", mId.substring(0, mLength))
        .toString();
  }
}
