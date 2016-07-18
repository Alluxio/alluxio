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
 * Method options for mkdirs in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class MkdirsOptions {
  // Permission to set for the directories being created.
  private Permission mPermission;
  // Determine whether to create any necessary but nonexistent parent directories.
  private boolean mCreateParent;

  /**
   * Constructs a default {@link MkdirsOptions}.
   */
  public MkdirsOptions() {
    mPermission = Permission.defaults().applyDirectoryUMask();
    // By default create parent is true.
    mCreateParent = true;
  }

  /**
   * @return whether to create any necessary but nonexistent parent directories
   */
  public boolean getCreateParent() {
    return mCreateParent;
  }

  /**
   * @return the permission
   */
  public Permission getPermission() {
    return mPermission;
  }

  /**
   * Sets the block size.
   *
   * @param createParent if true, creates any necessary but nonexistent parent directories
   * @return the updated option object
   */
  public MkdirsOptions setCreateParent(boolean createParent) {
    mCreateParent = createParent;
    return this;
  }

  /**
   * Sets the permission.
   *
   * @param permission the permission stats to set
   * @return the updated option object
   */
  public MkdirsOptions setPermission(Permission permission) {
    mPermission = permission;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MkdirsOptions)) {
      return false;
    }
    MkdirsOptions that = (MkdirsOptions) o;
    return Objects.equal(mPermission, that.mPermission)
        && Objects.equal(mCreateParent, that.mCreateParent);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPermission, mCreateParent);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("permission", mPermission)
        .add("createParent", mCreateParent)
        .toString();
  }
}
