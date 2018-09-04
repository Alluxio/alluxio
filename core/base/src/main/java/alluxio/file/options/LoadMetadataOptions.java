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

import alluxio.underfs.UfsStatus;

import com.google.common.base.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for loading metadata.
 */
@NotThreadSafe
public class LoadMetadataOptions {
  protected CommonOptions mCommonOptions;
  protected boolean mCreateAncestors;
  protected DescendantType mLoadDescendantType;
  protected UfsStatus mUfsStatus;

  protected LoadMetadataOptions() {}

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the type of descendants to load
   */
  public DescendantType getLoadDescendantType() {
    return mLoadDescendantType;
  }

  /**
   * @return null if unknown, else the status of UFS path for which loading metadata
   */
  @Nullable
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isCreateAncestors() {
    return mCreateAncestors;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public <T extends LoadMetadataOptions> T setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return (T) this;
  }

  /**
   * Sets the recursive flag.
   *
   * @param createAncestors the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public <T extends LoadMetadataOptions> T setCreateAncestors(boolean createAncestors) {
    mCreateAncestors = createAncestors;
    return (T) this;
  }

  /**
   * @param loadDescendantType the type of descendants to load
   * @return the updated object
   */
  public <T extends LoadMetadataOptions> T setLoadDescendantType(DescendantType loadDescendantType) {
    mLoadDescendantType = loadDescendantType;
    return (T) this;
  }

  /**
   * Sets the UFS status of path.
   *
   * @param status UFS status of path
   * @return the updated object
   */
  public <T extends LoadMetadataOptions> T setUfsStatus(UfsStatus status) {
    mUfsStatus = status;
    return (T) this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadMetadataOptions)) {
      return false;
    }
    LoadMetadataOptions that = (LoadMetadataOptions) o;
    return Objects.equal(mCreateAncestors, that.mCreateAncestors)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mLoadDescendantType, that.mLoadDescendantType)
        && Objects.equal(mUfsStatus, that.mUfsStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateAncestors, mLoadDescendantType, mUfsStatus, mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("createAncestors", mCreateAncestors)
        .add("loadDescendantLevels", mLoadDescendantType)
        .add("ufsStatus", mUfsStatus).toString();
  }
}
