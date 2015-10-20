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

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.NativeStorageType;
import tachyon.conf.TachyonConf;

@PublicApi
public final class InStreamOptions {
  public static class Builder {
    private NativeStorageType mNativeStorageType;

    /**
     * Creates a new builder for {@link InStreamOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mNativeStorageType =
          conf.getEnum(Constants.USER_FILE_NATIVE_STORAGE_TYPE_DEFAULT, NativeStorageType.class);
    }

    /**
     * @param nativeStorageType the Tachyon storage type to use
     * @return the builder
     */
    public Builder setTachyonStorageType(NativeStorageType nativeStorageType) {
      mNativeStorageType = nativeStorageType;
      return this;
    }

    /**
     * Builds a new instance of {@code InStreamOptions}.
     *
     * @return a {@code InStreamOptions} instance
     */
    public InStreamOptions build() {
      return new InStreamOptions(this);
    }
  }

  private final NativeStorageType mNativeStorageType;

  /**
   * @return the default {@code InStreamOptions}
   */
  public static InStreamOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private InStreamOptions(InStreamOptions.Builder builder) {
    mNativeStorageType = builder.mNativeStorageType;
  }

  /**
   * @return the Tachyon storage type
   */
  public NativeStorageType getTachyonStorageType() {
    return mNativeStorageType;
  }
}
