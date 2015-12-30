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

package tachyon.client.file.options;

import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.thrift.CompleteFileTOptions;

/**
 * The method option for completing a file.
 */
public final class CompleteFileOptions {

  /**
   * Builder for {@link CompleteFileOptions}.
   */
  public static class Builder {
    private long mUfsLength;

    /**
     * Creates a new builder for {@link CompleteFileOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mUfsLength = 0;
    }

    /**
     * @param ufsLength the UFS file length to us
     * @return the builder
     */
    public Builder setUfsLength(long ufsLength) {
      mUfsLength = ufsLength;
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
    return new Builder(ClientContext.getConf()).build();
  }

  private final long mUfsLength;

  private CompleteFileOptions(CompleteFileOptions.Builder builder) {
    mUfsLength = builder.mUfsLength;
  }

  /**
   * @return the UFS file length
   */
  public long getUfsLength() {
    return mUfsLength;
  }

  /**
   * @return Thrift representation of the options
   */
  public CompleteFileTOptions toThrift() {
    CompleteFileTOptions options = new CompleteFileTOptions();
    options.setUfsLength(mUfsLength);
    return options;
  }
}
