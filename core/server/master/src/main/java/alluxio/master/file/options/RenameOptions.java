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

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for completing a file.
 */
@NotThreadSafe
public final class RenameOptions {
  private long mOperationTimeMs;

  /**
   * @return the default {@link RenameOptions}
   */
  public static RenameOptions defaults() {
    return new RenameOptions();
  }

  private RenameOptions() {
    mOperationTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public RenameOptions setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
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
    return mOperationTimeMs == that.mOperationTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mOperationTimeMs);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("operationTimeMs", mOperationTimeMs)
        .toString();
  }
}
