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
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.security.authorization.Permission;
import alluxio.thrift.CreateUfsFileTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for creating a UFS file. Currently we do not allow user to set arbitrary owner and
 * group options. The owner and group will be set to the user login.
 */
@PublicApi
@NotThreadSafe
public final class CreateUfsFileOptions {
  /** The ufs owner this file should be owned by. */
  private String mOwner;
  /** The ufs group this file should be owned by. */
  private String mGroup;
  /** The ufs mode in short format, e.g. 0777. */
  private short mMode;

  /**
   * @return the default {@link CreateUfsFileOptions}
   * @throws IOException if failed to set owner from login module
   */
  public static CreateUfsFileOptions defaults() throws IOException {
    return new CreateUfsFileOptions();
  }

  private CreateUfsFileOptions() throws IOException {
    Permission perm = Permission.defaults();
    // Set owner and group from user login module, apply default file UMask.
    perm.setUserFromLoginModule(ClientContext.getConf()).applyFileUMask(ClientContext.getConf());
    // TODO(chaomin): set permission based on the alluxio file. Not needed for now since the
    // file is always created with default permission.

    mOwner = perm.getUserName();
    mGroup = perm.getGroupName();
    mMode = perm.getMode().toShort();
  }

  /**
   * @return the group which should own the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the owner who should own the file
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the ufs mode in short format, e.g. 0777
   */
  public short getMode() {
    return mMode;
  }

  /**
   * @return if the group has been set
   */
  public boolean hasGroup() {
    return mGroup != null;
  }

  /**
   * @return if the owner has been set
   */
  public boolean hasOwner() {
    return mOwner != null;
  }

  /**
   * @return if the mode has been set
   */
  public boolean hasMode() {
    return mMode != Constants.INVALID_MODE;
  }

  /**
   * @param owner the owner to be set
   * @return the updated options object
   */
  public CreateUfsFileOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * @param group the group to be set
   * @return the updated options object
   */
  public CreateUfsFileOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * @param mode the mode to be set
   * @return the updated options object
   */
  public CreateUfsFileOptions setMode(short mode) {
    mMode = mode;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateUfsFileOptions)) {
      return false;
    }
    CreateUfsFileOptions that = (CreateUfsFileOptions) o;
    return Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateUfsFileTOptions toThrift() {
    CreateUfsFileTOptions options = new CreateUfsFileTOptions();
    if (hasOwner()) {
      options.setOwner(mOwner);
    }
    if (hasGroup()) {
      options.setGroup(mGroup);
    }
    if (hasMode()) {
      options.setMode(mMode);
    }
    return options;
  }
}
