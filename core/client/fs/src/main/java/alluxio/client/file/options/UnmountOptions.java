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

import alluxio.annotation.PublicApi;
import alluxio.thrift.UnmountTOptions;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for unmounting a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class UnmountOptions {
  private CommonOptions mCommonOptions;

  /**
   * @return the default {@link UnmountOptions}
   */
  public static UnmountOptions defaults() {
    return new UnmountOptions();
  }

  private UnmountOptions() {
    mCommonOptions = CommonOptions.defaults();
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public UnmountOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof UnmountOptions)) {
      return false;
    }
    UnmountOptions that = (UnmountOptions) o;
    return Objects.equal(mCommonOptions, that.mCommonOptions);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public UnmountTOptions toThrift() {
    UnmountTOptions options = new UnmountTOptions();
//    options.setCommonOptions(mCommonOptions.toThrift());
    return options;
  }
}
