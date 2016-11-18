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

package alluxio.underfs.options;

import alluxio.annotation.PublicApi;
import alluxio.security.authorization.Permission;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class CreateOptions {
  // Permission to set for the file being created.
  private Permission mPermission;

  /**
   * @return the default {@link CreateOptions}
   */
  public static CreateOptions defaults() {
    return new CreateOptions();
  }

  /**
   * Constructs a default {@link CreateOptions}.
   */
  private CreateOptions() {
    mPermission = Permission.defaults().applyFileUMask();
  }

  /**
   * @return the permission
   */
  public Permission getPermission() {
    return mPermission;
  }

  /**
   * Sets the permission.
   *
   * @param permission the permission stats to set
   * @return the updated option object
   */
  public CreateOptions setPermission(Permission permission) {
    mPermission = permission;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateOptions)) {
      return false;
    }
    CreateOptions that = (CreateOptions) o;
    return Objects.equal(mPermission, that.mPermission);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPermission);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("permission", mPermission)
        .toString();
  }
}
