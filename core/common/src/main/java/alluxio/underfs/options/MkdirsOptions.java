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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.authorization.Mode;
import alluxio.util.ModeUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for mkdirs in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class MkdirsOptions {
  // Determine whether to create any necessary but nonexistent parent directories.
  private boolean mCreateParent;

  private String mOwner;
  private String mGroup;
  private Mode mMode;

  /**
   * @param conf Alluxio configuration
   * @return the default {@link MkdirsOptions}
   */
  public static MkdirsOptions defaults(AlluxioConfiguration conf) {
    return new MkdirsOptions(conf.get(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK));
  }

  /**
   * Constructs a default {@link MkdirsOptions}.
   */
  private MkdirsOptions(String authUmask) {
    // By default create parent is true.
    mCreateParent = true;
    // default owner and group are null (unset)
    mOwner = null;
    mGroup = null;
    mMode = ModeUtils.applyDirectoryUMask(Mode.defaults(), authUmask);
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
   * Sets option to create parent directories.
   *
   * @param createParent if true, creates any necessary but nonexistent parent directories
   * @return the updated option object
   */
  public MkdirsOptions setCreateParent(boolean createParent) {
    mCreateParent = createParent;
    return this;
  }

  /**
   * @param owner the owner to set
   * @return the updated object
   */
  public MkdirsOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to set
   * @return the updated object
   */
  public MkdirsOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode to set
   * @return the updated object
   */
  public MkdirsOptions setMode(Mode mode) {
    mMode = mode;
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
    return Objects.equal(mCreateParent, that.mCreateParent)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateParent, mOwner, mGroup, mMode);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("createParent", mCreateParent)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .toString();
  }
}
