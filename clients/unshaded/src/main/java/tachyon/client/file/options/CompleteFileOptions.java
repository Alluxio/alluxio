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

import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.thrift.CompleteFileTOptions;

@PublicApi
public final class CompleteFileOptions {
  public static class Builder {
    private long mLength;

    /**
     * Creates a new builder for {@link CompleteFileOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mLength = 0;
    }

    /**
     * @param length the file length to us
     * @return the builder
     */
    public Builder setLength(long length) {
      mLength = length;
      return this;
    }

    /**
     * Builds a new instance of {@code CompleteFileOptions}.
     *
     * @return a {@code CompleteFileOptions} instance
     */
    public CompleteFileOptions build() {
      return new CompleteFileOptions(this);
    }
  }

  /**
   * @return the default {@code CompleteFileOptions}
   */
  public static CompleteFileOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final long mLength;

  private CompleteFileOptions(CompleteFileOptions.Builder builder) {
    mLength = builder.mLength;
  }

  /**
   * @return the file length
   */
  public long getLength() {
    return mLength;
  }

  /**
   * @return Thrift representation of the options
   */
  public CompleteFileTOptions toThrift() {
    CompleteFileTOptions options = new CompleteFileTOptions();
    options.setLength(mLength);
    return options;
  }
}
