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

<<<<<<< HEAD
||||||| merged common ancestors
import alluxio.thrift.CompleteFileTOptions;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

=======
import alluxio.thrift.CompleteFileTOptions;
import alluxio.underfs.UfsStatus;
import alluxio.wire.CommonOptions;

import com.google.common.base.Objects;

>>>>>>> master
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
<<<<<<< HEAD
public final class CompleteFileOptions extends alluxio.file.options.CompleteFileOptions{
||||||| merged common ancestors
public final class CompleteFileOptions {
  private CommonOptions mCommonOptions;
  private long mUfsLength;
  private long mOperationTimeMs;

=======
public final class CompleteFileOptions {
  private CommonOptions mCommonOptions;
  private long mUfsLength;
  private long mOperationTimeMs;
  private UfsStatus mUfsStatus;

>>>>>>> master
  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new CompleteFileOptions();
  }

  private CompleteFileOptions() {
    mCommonOptions = CommonOptions.defaults();
    mUfsLength = 0;
    mOperationTimeMs = System.currentTimeMillis();
    mUfsStatus = null;
  }
<<<<<<< HEAD
||||||| merged common ancestors

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
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
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

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public CompleteFileOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
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
    return Objects.equal(mUfsLength, that.mUfsLength)
        && Objects.equal(mCommonOptions, that.mCommonOptions)
        && mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mUfsLength, mOperationTimeMs, mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("ufsLength", mUfsLength)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
=======

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
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the ufs status
   */
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
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

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public CompleteFileOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return this;
  }

  /**
   * @param ufsStatus the ufs status to use
   * @return the updated options object
   */
  public CompleteFileOptions setUfsStatus(UfsStatus ufsStatus) {
    mUfsStatus = ufsStatus;
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
>>>>>>> master
}
