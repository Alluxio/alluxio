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
import tachyon.client.UnderStorageType;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.thrift.MkdirTOptions;

@PublicApi
public final class MkdirOptions {
  public static class Builder {
    private boolean mRecursive;
    private UnderStorageType mUnderStorageType;

    /**
     * Creates a new builder for {@link MkdirOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mRecursive = false;
      WriteType defaultWriteType =
          conf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
      mUnderStorageType = defaultWriteType.getUnderStorageType();
    }

    /**
     * @param recursive the recursive flag value to use; it specifies whether parent directories
     *        should be created if they do not already exist
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * This is an advanced API, use {@link Builder#setWriteType} when possible.
     *
     * @param underStorageType the under storage type to use
     * @return the builder
     */
    public Builder setUnderStorageType(UnderStorageType underStorageType) {
      mUnderStorageType = underStorageType;
      return this;
    }

    /**
     * @param writeType the write type to use
     * @return the builder
     */
    public Builder setWriteType(WriteType writeType) {
      mUnderStorageType = writeType.getUnderStorageType();
      return this;
    }

    /**
     * Builds a new instance of {@code MkdirOptions}.
     *
     * @return a {@code MkdirOptions} instance
     */
    public MkdirOptions build() {
      return new MkdirOptions(this);
    }
  }

  /**
   * @return the default {@code MkdirOptions}
   */
  public static MkdirOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final boolean mRecursive;
  private final UnderStorageType mUnderStorageType;

  private MkdirOptions(MkdirOptions.Builder builder) {
    mRecursive = builder.mRecursive;
    mUnderStorageType = builder.mUnderStorageType;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  /**
   * @return Thrift representation of the options
   */
  public MkdirTOptions toThrift() {
    MkdirTOptions options = new MkdirTOptions();
    options.setPersisted(mUnderStorageType.isSyncPersist());
    options.setRecursive(mRecursive);
    return options;
  }
}
