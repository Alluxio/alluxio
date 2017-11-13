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

import alluxio.thrift.CompleteFileTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
public final class CompleteFileOptions extends CommonOptions {
  private long mUfsLength;
  private long mOperationTimeMs;

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new CompleteFileOptions();
  }

  /**
   * Creates a new instance of {@link CompleteFileOptions} from {@link CompleteFileTOptions}.
   *
   * @param options Thrift options
   */
  public CompleteFileOptions(CompleteFileTOptions options) {
    super(options != null ? options.getCommonOptions() : null);
    mUfsLength = 0;
    mOperationTimeMs = System.currentTimeMillis();
    if (options != null) {
      mUfsLength = options.getUfsLength();
    }
  }

  private CompleteFileOptions() {
    this(null);
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
    if (!(super.equals(o))) {
      return false;
    }
    CompleteFileOptions that = (CompleteFileOptions) o;
    return Objects.equal(mUfsLength, that.mUfsLength) && mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hashCode(mUfsLength, mOperationTimeMs);
  }

  @Override
  public String toString() {
    return toStringHelper()
        .add("ufsLength", mUfsLength)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
}
