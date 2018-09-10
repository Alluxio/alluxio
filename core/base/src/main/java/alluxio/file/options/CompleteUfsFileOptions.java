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

package alluxio.file.options;

import alluxio.security.authorization.Mode;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for completing a UFS file. Currently we do not allow users to set arbitrary owner and
 * group options. The owner and group will be set to the user login.
 *
 * @param <T> the type of the concrete subclass
 */
@NotThreadSafe
public abstract class CompleteUfsFileOptions<T extends CompleteUfsFileOptions> {
  protected String mOwner;
  protected String mGroup;
  protected Mode mMode;

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
   * @param owner the owner to set
   * @return the updated object
   */
  public T setOwner(String owner) {
    mOwner = owner;
    return (T) this;
  }

  /**
   * @param group the group to set
   * @return the updated object
   */
  public T setGroup(String group) {
    mGroup = group;
    return (T) this;
  }

  /**
   * @param mode the mode to set
   * @return the updated object
   */
  public T setMode(Mode mode) {
    mMode = mode;
    return (T) this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompleteUfsFileOptions)) {
      return false;
    }
    CompleteUfsFileOptions that = (CompleteUfsFileOptions) o;
    return Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOwner, mGroup, mMode);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .toString();
  }
}
