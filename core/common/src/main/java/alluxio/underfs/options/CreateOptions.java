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
import alluxio.security.authorization.Mode;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a file in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class CreateOptions {
  // Determine whether to create any necessary but nonexistent parent directories.
  // When setting permissions, this option should be = false to remain in sync w/ master
  private boolean mCreateParent;

  // Ensure writes are not readable till close.
  private boolean mEnsureAtomic;

  private String mOwner;
  private String mGroup;
  private Mode mMode;

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
    mOwner = "";
    mGroup = "";
    mMode = Mode.defaults().applyFileUMask();
  }

  /**
   * @return whether to create any necessary but nonexistent parent directories
   */
  public boolean getCreateParent() {
    return mCreateParent;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return true, if writes are guaranteed to be atomic
   */
  public boolean isEnsureAtomic() {
    return mEnsureAtomic;
  }

  /**
   * Sets option to force creation of parent directories. If true, any necessary but nonexistent
   * parent directories are created. If false, the behavior is implementation dependent.
   *
   * @param createParent option to force parent directory creation
   * @return the updated object
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
   * @return the updated object
   */
  public CreateOptions setEnsureAtomic(boolean atomic) {
    mEnsureAtomic = atomic;
    return this;
  }

  /**
   * @param owner the owner to set
   * @return the updated object
   */
  public CreateOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to set
   * @return the updated object
   */
  public CreateOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode to set
   * @return the updated object
   */
  public CreateOptions setMode(Mode mode) {
    mMode = mode;
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
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateParent, mEnsureAtomic, mOwner, mGroup, mMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("createParent", mCreateParent)
        .add("ensureAtomic", mEnsureAtomic)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .toString();
  }
}
