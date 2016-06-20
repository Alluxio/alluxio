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

import alluxio.Configuration;
import alluxio.annotation.PublicApi;
import alluxio.security.authorization.PermissionStatus;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for mkdirs in UnderFileSystem.
 */
@PublicApi
@NotThreadSafe
public final class MkdirsOptions {
  // Permission status to set for the directories being created.
  private PermissionStatus mPermissionStatus;
  // Determine whether to create any necessary but nonexistent parent directories.
  private boolean mCreateParent;

  /**
   * @return the default {@link MkdirsOptions}
   */
  public static MkdirsOptions defaults() {
    return new MkdirsOptions();
  }

  private MkdirsOptions() {
    mPermissionStatus = PermissionStatus.defaults();
    // By default create parent is true.
    mCreateParent = true;
  }

  /**
   * Constructs a {@link MkdirsOptions} with specified configuration.
   *
   * @param conf the configuration
   */
  public MkdirsOptions(Configuration conf) {
    // Only set the permission, not the owner/group because owner/group is not yet used for UFS
    // directories creation.
    mPermissionStatus = PermissionStatus.defaults().applyDirectoryUMask(conf);
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
   * @return the permission status
   */
  public PermissionStatus getPermissionStatus() {
    return mPermissionStatus;
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
   * Sets the permission status.
   *
   * @param permissionStatus the permission stats to set
   * @return the updated option object
   */
  public MkdirsOptions setPermissionStatus(PermissionStatus permissionStatus) {
    mPermissionStatus = permissionStatus;
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
    MkdirsOptions that = (MkdirsOptions) o;
    return Objects.equal(mPermissionStatus, that.mPermissionStatus)
        && Objects.equal(mCreateParent, that.mCreateParent);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPermissionStatus, mCreateParent);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("permissionStatus", mPermissionStatus)
        .add("createParent", mCreateParent)
        .toString();
  }
}
