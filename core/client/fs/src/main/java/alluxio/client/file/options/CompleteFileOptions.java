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

import alluxio.thrift.CompleteFileTOptions;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
public final class CompleteFileOptions {
  private CommonOptions mCommonOptions;
  private long mUfsLength;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new CompleteFileOptions();
  }

  private CompleteFileOptions() {
    mCommonOptions = CommonOptions.defaults();
    mUfsLength = 0;
  }

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
  }

  /**
   * @return the UFS file length
   */
  public long getUfsLength() {
    return mUfsLength;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public CompleteFileOptions setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return this;
  }

  /**
   * @param ufsLength the UFS file length to use
   * @return the updated options object
   */
  public CompleteFileOptions setUfsLength(long ufsLength) {
    mUfsLength = ufsLength;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompleteFileOptions)) {
      return false;
    }
    CompleteFileOptions that = (CompleteFileOptions) o;
    return Objects.equal(mCommonOptions, that.mCommonOptions)
        && Objects.equal(mUfsLength, that.mUfsLength);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommonOptions, mUfsLength);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("ufsLength", mUfsLength)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CompleteFileTOptions toThrift() {
    CompleteFileTOptions options = new CompleteFileTOptions();
    options.setCommonOptions(mCommonOptions.toThrift());
    options.setUfsLength(mUfsLength);
    return options;
  }
}
