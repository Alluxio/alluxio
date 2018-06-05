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
import alluxio.thrift.RenameTOptions;
import alluxio.wire.CommonOptions;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for renaming a file or a directory.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class RenameOptions {
  private CommonOptions mCommonOptions;

  /**
   * @return the default {@link RenameOptions}
   */
  public static RenameOptions defaults() {
    return new RenameOptions();
  }

  private RenameOptions() {
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
  public RenameOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RenameOptions)) {
      return false;
    }
    RenameOptions that = (RenameOptions) o;
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
  public RenameTOptions toThrift() {
    RenameTOptions options = new RenameTOptions();
//    options.setCommonOptions(mCommonOptions.toThrift());
    return options;
  }
}
