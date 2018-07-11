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

import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;
import alluxio.underfs.UfsStatus;
import alluxio.util.SecurityUtils;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions extends CreatePathOptions<CreateDirectoryOptions> {
  private boolean mAllowExists;
  private UfsStatus mUfsStatus;

  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  /**
   * Constructs an instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   * The option of permission is constructed with the username obtained from thrift
   * transport.
   *
   * @param options the {@link CreateDirectoryTOptions} to use
   */
  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      mAllowExists = options.isAllowExists();
      mPersisted = options.isPersisted();
      mRecursive = options.isRecursive();
      if (SecurityUtils.isAuthenticationEnabled()) {
        mOwner = SecurityUtils.getOwnerFromThriftClient();
        mGroup = SecurityUtils.getGroupFromThriftClient();
      }
      if (options.isSetMode()) {
        mMode = new Mode(options.getMode());
      } else {
        mMode.applyDirectoryUMask();
      }
    }
  }

  private CreateDirectoryOptions() {
    super();
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

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the object being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param ufsStatus the {@link UfsStatus}; It sets the optional ufsStatus as an optimization
   * @return the updated options object
   */
  public CreateDirectoryOptions setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return getThis();
  }

  @Override
  protected CreateDirectoryOptions getThis() {
    return this;
  }

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
}
