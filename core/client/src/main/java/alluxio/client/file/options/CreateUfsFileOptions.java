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
import alluxio.security.authorization.PermissionStatus;
import alluxio.thrift.CreateUfsFileTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for creating a UFS file. Currently we do not allow users to set arbitrary user and
 * group options. The user and group will be set to the user login.
 */
@PublicApi
@NotThreadSafe
public final class CreateUfsFileOptions {
  /** The ufs user this file should be owned by. */
  private String mUser;
  /** The ufs group this file should be owned by. */
  private String mGroup;
  /** The ufs permission in short format, e.g. 0777. */
  private short mPermission;

  /**
   * @return the default {@link CreateUfsFileOptions}
   * @throws IOException if failed to set user from login module
   */
  public static CreateUfsFileOptions defaults() throws IOException {
    return new CreateUfsFileOptions();
  }

  private CreateUfsFileOptions() throws IOException {
    PermissionStatus ps = PermissionStatus.defaults();
    // Set user and group from user login module, apply default file UMask.
    ps.setUserFromLoginModule(ClientContext.getConf()).applyFileUMask(ClientContext.getConf());
    // TODO(chaomin): set permission based on the alluxio file. Not needed for now since the
    // file is always created with default permission.

    mUser = ps.getUserName();
    mGroup = ps.getGroupName();
    mPermission = ps.getPermission().toShort();
  }

  /**
   * @return the group which should own the file
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the user who should own the file
   */
  public String getUser() {
    return mUser;
  }

  /**
   * @return the ufs permission in short format, e.g. 0777
   */
  public short getPermission() {
    return mPermission;
  }

  /**
   * @return if the group has been set
   */
  public boolean hasGroup() {
    return mGroup != null;
  }

  /**
   * @return if the user has been set
   */
  public boolean hasUser() {
    return mUser != null;
  }

  /**
   * @return if the permission has been set
   */
  public boolean hasPermission() {
    return mPermission != Constants.INVALID_PERMISSION;
  }

  /**
   * @param user the user to be set
   * @return the updated options object
   */
  public CreateUfsFileOptions setUser(String user) {
    mUser = user;
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
   * @param permission the permission to be set
   * @return the updated options object
   */
  public CreateUfsFileOptions setPermission(short permission) {
    mPermission = permission;
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
    return Objects.equal(mUser, that.mUser)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mPermission, that.mPermission);
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
    if (hasGroup()) {
      options.setGroup(mGroup);
    }
    if (hasUser()) {
      options.setUser(mUser);
    }
    if (hasPermission()) {
      options.setPermission(mPermission);
    }
    return options;
  }
}
