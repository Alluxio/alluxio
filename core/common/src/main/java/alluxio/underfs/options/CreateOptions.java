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
  // Determine whether to create any necessary but nonexistent parent directories.
  private boolean mCreateParent;

  // Ensure writes are not readable till close.
  private boolean mEnsureAtomic;

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
    mCreateParent = false;
    mEnsureAtomic = true;
    mPermission = Permission.defaults().applyFileUMask();
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
   * @return true, if writes are guaranteed to be atomic
   */
  public boolean isEnsureAtomic() {
    return mEnsureAtomic;
  }

  /**
   * Sets option to create parent directories.
   *
   * @param createParent if true, creates any necessary but nonexistent parent directories
   * @return the updated option object
   */
  public CreateOptions setCreateParent(boolean createParent) {
    mCreateParent = createParent;
    return this;
  }

  /**
   * Set atomicity guarantees. When true, writes to the created stream must become readable all at
   * once or not at all. The destination path is created only after closing the given stream. When,
   * false the stream may or may not be atomic.
   *
   * @param atomic whether to ensure created stream is atomic
   * @return the updated option object
   */
  public CreateOptions setEnsureAtomic(boolean atomic) {
    mEnsureAtomic = atomic;
    return this;
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
    return (mCreateParent == that.mCreateParent)
        && (mEnsureAtomic == that.mEnsureAtomic)
        && Objects.equal(mPermission, that.mPermission);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateParent, mEnsureAtomic, mPermission);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("createParent", mCreateParent)
        .add("ensureAtomic", mEnsureAtomic)
        .add("permission", mPermission)
        .toString();
  }
}
