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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
public abstract class RenameOptions {
  protected CommonOptions mCommonOptions;
  protected long mOperationTimeMs;

  /**
   * @return the common options
   */
  public CommonOptions getCommonOptions() {
    return mCommonOptions;
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
  public <T extends RenameOptions> T setCommonOptions(CommonOptions options) {
    mCommonOptions = options;
    return (T) this;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public <T extends RenameOptions> T setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return (T) this;
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
    return Objects.equal(mCommonOptions, that.mCommonOptions)
        && mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOperationTimeMs, mCommonOptions);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("commonOptions", mCommonOptions)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
}
