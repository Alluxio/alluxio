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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
public class CompleteFileOptions {
  protected CommonOptions mCommonOptions;
  protected long mUfsLength;
  protected long mOperationTimeMs;
  protected UfsStatus mUfsStatus;

  protected CompleteFileOptions() {}

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
   * @return the ufs status
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @param options the common options
   * @return the updated options object
   */
  public <T extends CompleteFileOptions> T setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return (T) this;
  }

  /**
   * @param ufsLength the UFS file length to use
   * @return the updated options object
   */
  public <T extends CompleteFileOptions> T setUfsLength(long ufsLength) {
    mUfsLength = ufsLength;
    return (T) this;
  }

  /**
   * @param ufsStatus the ufs status to use
   * @return the updated options object
   */
  public <T extends CompleteFileOptions> T setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
    return (T) this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public <T extends CompleteFileOptions> T setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return (T) this;
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
    return Objects.equal(mUfsLength, that.mUfsLength)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && mOperationTimeMs == that.mOperationTimeMs
        && Objects.equal(mUfsStatus, that.mUfsStatus);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUfsLength, mOperationTimeMs, mCommonOptions, mUfsStatus);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("ufsLength", mUfsLength)
        .add("operationTimeMs", mOperationTimeMs)
        .add("ufsStatus", mUfsStatus)
        .toString();
  }
}
