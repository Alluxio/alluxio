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

import alluxio.annotation.PublicApi;
import alluxio.thrift.ListStatusTOptions;
import alluxio.thrift.LoadMetadataType;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for get file info list.
 */
@PublicApi
@NotThreadSafe
public final class GetFileInfoListOptions {
  private LoadMetadataType mLoadMetadataType;

  /**
   * @return the default {@link GetFileInfoListOptions}
   */
  public static GetFileInfoListOptions defaults() {
    return new GetFileInfoListOptions();
  }

  private GetFileInfoListOptions() {
    mLoadMetadataType = LoadMetadataType.Once;
  }

  /**
   * Create an instance of {@link GetFileInfoListOptions} from a {@link ListStatusTOptions}.
   *
   * @param options the thrift representation of list status options
   */
  public GetFileInfoListOptions(ListStatusTOptions options) {
    mLoadMetadataType = options.getLoadMetadataType();
  }

  /**
   * @return the load metadata type. It specifies whether the direct children should
   *         be loaded from UFS in different scenarios.
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * Sets the {@link GetFileInfoListOptions#mLoadMetadataType}.
   *
   * @param loadMetadataType the load metadata type
   * @return the updated options
   */
  public GetFileInfoListOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetFileInfoListOptions)) {
      return false;
    }
    GetFileInfoListOptions that = (GetFileInfoListOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadMetadataType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("loadMetadataType", mLoadMetadataType.toString())
        .toString();
  }
}
