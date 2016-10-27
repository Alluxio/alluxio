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
import alluxio.security.authorization.Permission;
import alluxio.thrift.CompleteUfsFileTOptions;

import com.google.common.base.Objects;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Options for completing a UFS file. Currently we do not allow users to set arbitrary owner and
 * group options. The owner and group will be set to the user login.
 */
@PublicApi
@NotThreadSafe
public final class CompleteUfsFileOptions {
  /** The ufs file permission, including owner, group and mode. */
  private Permission mPermission;

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
    mPermission = Permission.defaults();
    // Set owner and group from user login module, apply default file UMask.
    mPermission.setOwnerFromLoginModule().applyFileUMask();
    // TODO(chaomin): set permission based on the alluxio file. Not needed for now since the
    // file is always created with default permission.
  }

  /**
   * @return the permission of the UFS file
   */
  public Permission getPermission() {
    return mPermission;
  }

  /**
   * @param permission the permission to be set
   * @return the updated options object
   */
  public CompleteUfsFileOptions setPermission(Permission permission) {
    mPermission = permission;
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

  /**
   * @return Thrift representation of the options
   */
  public CompleteUfsFileTOptions toThrift() {
    CompleteUfsFileTOptions options = new CompleteUfsFileTOptions();
    if (!mPermission.getOwner().isEmpty()) {
      options.setOwner(mPermission.getOwner());
    }
    if (!mPermission.getGroup().isEmpty()) {
      options.setGroup(mPermission.getGroup());
    }
    short mode = mPermission.getMode().toShort();
    if (mode != Constants.INVALID_MODE) {
      options.setMode(mode);
    }
    return options;
  }
}
