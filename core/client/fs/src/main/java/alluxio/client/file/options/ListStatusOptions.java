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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.thrift.ListStatusTOptions;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for listing the status.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class ListStatusOptions {
  private CommonOptions mCommonOptions;
  private LoadMetadataType mLoadMetadataType;
  private boolean mRecursive;

  /**
   * @return the default {@link ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new ListStatusOptions();
  }

  private ListStatusOptions() {
    mCommonOptions = CommonOptions.defaults();
    mLoadMetadataType =
        Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class);
    mRecursive = false;
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the load metadata type. It specifies whether the direct children should
   *         be loaded from UFS in different scenarios.
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * @return whether the command should recursively list the status of the underlying
   *         directories.
   */
  public boolean getRecursive() {
    return mRecursive;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public ListStatusOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * @param loadMetadataType the loadMetataType
   * @return the updated options
   */
  public ListStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  /**
   * @param recursive recursive or not
   * @return the updated options object
   */
  public ListStatusOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListStatusOptions)) {
      return false;
    }
    ListStatusOptions that = (ListStatusOptions) o;
    return Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mLoadMetadataType, that.mLoadMetadataType)
        && Objects.equal(mRecursive, that.mRecursive);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions, mLoadMetadataType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("loadMetadataType", mLoadMetadataType.toString())
        .toString();
  }

  /**
   * @return thrift representation of the options
   */
  public ListStatusTOptions toThrift() {
    ListStatusTOptions options = new ListStatusTOptions();
    options.setLoadDirectChildren(
        mLoadMetadataType == LoadMetadataType.Once || mLoadMetadataType == LoadMetadataType.Always);

    options.setLoadMetadataType(LoadMetadataType.toThrift(mLoadMetadataType));
    options.setCommonOptions(mCommonOptions.toThrift());
    options.setRecursive(mRecursive);
    return options;
  }
}
