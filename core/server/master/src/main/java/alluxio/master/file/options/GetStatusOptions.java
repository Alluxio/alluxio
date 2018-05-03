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

import alluxio.Constants;
import alluxio.thrift.GetStatusTOptions;
import alluxio.wire.CommonOptions;
import alluxio.wire.LoadMetadataType;

import alluxio.wire.TtlAction;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getStatus.
 */
@NotThreadSafe
public final class GetStatusOptions {
  private CommonOptions mCommonOptions;
  private LoadMetadataType mLoadMetadataType;
  private long mTtl;
  private TtlAction mTtlAction;

  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    super();
    mCommonOptions = CommonOptions.defaults();
    mLoadMetadataType = LoadMetadataType.Once;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
  }

  /**
   * Create an instance of {@link GetStatusOptions} from a {@link GetStatusTOptions}.
   *
   * @param options the thrift representation of getFileInfo options
   */
  public GetStatusOptions(GetStatusTOptions options) {
    this();
    if (options != null) {
      if (options.isSetCommonOptions()) {
        mCommonOptions = new CommonOptions(options.getCommonOptions());
      }
      if (options.isSetLoadMetadataType()) {
        mLoadMetadataType = LoadMetadataType.fromThrift(options.getLoadMetadataType());
      }
      mTtl = options.getTtl();
      mTtlAction = TtlAction.fromThrift(options.getTtlAction());
    }
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the load metadata type
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * @return time to live
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return action after ttl expired
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public GetStatusOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * Sets the {@link GetStatusOptions#mLoadMetadataType}.
   *
   * @param loadMetadataType the load metadata type
   * @return the updated options
   */
  public GetStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  /**
   * @param ttl time to live
   */
  public void setTtl(long ttl) {
    mTtl = ttl;
  }

  /**
   * @param ttlAction action after ttl expired
   */
  public void setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetStatusOptions)) {
      return false;
    }
    GetStatusOptions that = (GetStatusOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadMetadataType, mCommonOptions, mTtl, mTtlAction);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("loadMetadataType", mLoadMetadataType.toString())
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction.toString())
        .toString();
  }
}
