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

package alluxio.client.file.options;

import alluxio.Constants;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CompleteUfsFileTOptions;
import alluxio.util.SecurityUtils;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for completing a UFS file. Currently we do not allow users to set arbitrary owner and
 * group options. The owner and group will be set to the user login.
 */
@NotThreadSafe
public final class CompleteUfsFileOptions {
  private String mOwner;
  private String mGroup;
  private Mode mMode;

  /**
   * Creates a default {@link CompleteUfsFileOptions} with owner, group from login module and
   * default file mode.
   *
   * @return the default {@link CompleteUfsFileOptions}
   * @throws IOException if failed to set owner from login module
   */
  public static CompleteUfsFileOptions defaults() throws IOException {
    return new CompleteUfsFileOptions();
  }

  private CompleteUfsFileOptions() throws IOException {
    mOwner = SecurityUtils.getOwnerFromLoginModule();
    mGroup = SecurityUtils.getGroupFromLoginModule();
    mMode = Mode.defaults().applyFileUMask();
    // TODO(chaomin): set permission based on the alluxio file. Not needed for now since the
    // file is always created with default permission.
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
   * @param owner the owner to set
   * @return the updated object
   */
  public CompleteUfsFileOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to set
   * @return the updated object
   */
  public CompleteUfsFileOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode to set
   * @return the updated object
   */
  public CompleteUfsFileOptions setMode(Mode mode) {
    mMode = mode;
    return this;
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

  /**
   * @return Thrift representation of the options
   */
  public CompleteUfsFileTOptions toThrift() {
    CompleteUfsFileTOptions options = new CompleteUfsFileTOptions();
    if (!mOwner.isEmpty()) {
      options.setOwner(mOwner);
    }
    if (!mGroup.isEmpty()) {
      options.setGroup(mGroup);
    }
    if (mMode != null && mMode.toShort() != Constants.INVALID_MODE) {
      options.setMode(mMode.toShort());
    }
    return options;
  }
}
