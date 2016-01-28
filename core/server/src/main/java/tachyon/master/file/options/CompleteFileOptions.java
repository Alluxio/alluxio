/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file.options;

import tachyon.conf.TachyonConf;
import tachyon.master.MasterContext;
import tachyon.thrift.CompleteFileTOptions;

/**
 * Method option for completing a file.
 */
public final class CompleteFileOptions {

  /**
   * Builder for {@link CompleteFileOptions}.
   */
  public static class Builder {
    private long mUfsLength;
    private long mOperationTimeMs;

    /**
     * Creates a new builder for {@link CompleteFileOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mUfsLength = 0;
      mOperationTimeMs = System.currentTimeMillis();
    }

    /**
     * @param ufsLength the UFS file length to use
     * @return the builder
     */
    public Builder setUfsLength(long ufsLength) {
      mUfsLength = ufsLength;
      return this;
    }

    /**
     * @param operationTimeMs the operation time to use
     * @return the builder
     */
    public Builder setOperationTimeMs(long operationTimeMs) {
      mOperationTimeMs = operationTimeMs;
      return this;
    }

    /**
     * Builds a new instance of {@link CompleteFileOptions}.
     *
     * @return a {@link CompleteFileOptions} instance
     */
    public CompleteFileOptions build() {
      return new CompleteFileOptions(this);
    }
  }

  /**
   * @return the default {@link CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private long mUfsLength;
  private long mOperationTimeMs;

  private CompleteFileOptions(CompleteFileOptions.Builder builder) {
    mUfsLength = builder.mUfsLength;
    mOperationTimeMs = builder.mOperationTimeMs;
  }

  /**
   * Creates a new instance of {@link CompleteFileOptions} from {@link CompleteFileOptions}.
   *
   * @param options Thrift options
   */
  public CompleteFileOptions(CompleteFileTOptions options) {
    mUfsLength = options.getUfsLength();
    mOperationTimeMs = System.currentTimeMillis();
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
}
