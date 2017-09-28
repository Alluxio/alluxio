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
import alluxio.wire.LoadMetadataType;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for checking the existence of a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class ExistsOptions {
  private LoadMetadataType mLoadMetadataType;

  /**
   * @return the default {@link ExistsOptions}
   */
  public static ExistsOptions defaults() {
    return new ExistsOptions();
  }

  private ExistsOptions() {
    mLoadMetadataType =
        Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class);
  }

  /**
   * @return the load metadata type
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * @param loadMetadataType the loadMetataType
   * @return the updated options
   */
  public ExistsOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExistsOptions)) {
      return false;
    }
    ExistsOptions that = (ExistsOptions) o;
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

  /**
   * @return the {@link GetStatusOptions} representation of these options
   */
  public GetStatusOptions toGetStatusOptions() {
    return GetStatusOptions.defaults().setLoadMetadataType(mLoadMetadataType);
  }
}
