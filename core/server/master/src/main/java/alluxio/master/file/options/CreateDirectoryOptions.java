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

package alluxio.master.file.options;

<<<<<<< HEAD
import alluxio.util.ModeUtils;
||||||| merged common ancestors
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.underfs.UfsStatus;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;
=======
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.underfs.UfsStatus;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
>>>>>>> master

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.List;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
<<<<<<< HEAD
public final class CreateDirectoryOptions
    extends alluxio.file.options.CreateDirectoryOptions<CreateDirectoryOptions> {
||||||| merged common ancestors
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;
  private UfsStatus mUfsStatus;

=======
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;
  private UfsStatus mUfsStatus;
  private List<AclEntry> mDefaultAcl;

>>>>>>> master
  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  private CreateDirectoryOptions() {
    super();
<<<<<<< HEAD
||||||| merged common ancestors
    mAllowExists = false;
    mMode.applyDirectoryUMask();
    mUfsStatus = null;
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the {@link UfsStatus}
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }
=======
    mAllowExists = false;
    mMode.applyDirectoryUMask();
    mUfsStatus = null;
    mDefaultAcl = Collections.emptyList();
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the default ACL in the form of a list of default ACL Entries
   */
  public List<AclEntry> getDefaultAcl() {
    return mDefaultAcl;
  }

  /**
   * Sets the default ACL in the option.
   * @param defaultAcl a list of default ACL Entries
   * @return the updated options object
   */
  public CreateDirectoryOptions setDefaultAcl(List<AclEntry> defaultAcl) {
    mDefaultAcl = ImmutableList.copyOf(defaultAcl);
    return getThis();
  }

  /**
   * @return the {@link UfsStatus}
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }
>>>>>>> master

    CreatePathOptionsUtils.setDefaults(this);

    mAllowExists = false;
    mMode = ModeUtils.applyDirectoryUMask(mMode);
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }
<<<<<<< HEAD
||||||| merged common ancestors

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mUfsStatus, that.mUfsStatus);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mAllowExists, mUfsStatus);
  }

  @Override
  public String toString() {
    return toStringHelper().add("allowExists", mAllowExists)
        .add("ufsStatus", mUfsStatus).toString();
  }
=======

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    if (!(super.equals(o))) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mUfsStatus, that.mUfsStatus)
        && Objects.equal(mDefaultAcl, that.mDefaultAcl);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mAllowExists, mUfsStatus, mDefaultAcl);
  }

  @Override
  public String toString() {
    return toStringHelper().add("allowExists", mAllowExists)
        .add("ufsStatus", mUfsStatus)
        .add("defaultAcl", mDefaultAcl).toString();
  }
>>>>>>> master
}
