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
import tachyon.conf.TachyonConf;
import tachyon.thrift.CreateTOptions;
import tachyon.thrift.MkdirTOptions;

@PublicApi
public final class CreateOptions {
  public static class Builder {
    // TODO(calvin): Should this just be an int?
    private long mBlockSizeBytes;
    private boolean mRecursive;
    private long mTTL;
    private UnderStorageType mUnderStorageType;

    /**
     * Creates a new builder for {@link CreateOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mBlockSizeBytes = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mRecursive = false;
      mTTL = Constants.NO_TTL;
      mUnderStorageType =
          conf.getEnum(Constants.USER_DEFAULT_UNDER_STORAGE_TYPE, UnderStorageType.class);
    }

    /**
     * @param blockSizeBytes the block size to use
     * @return the builder
     */
    public Builder setBlockSizeBytes(long blockSizeBytes) {
      mBlockSizeBytes = blockSizeBytes;
      return this;
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
     * @param ttl the TTL (time to live) value to use; it identifies duration (in seconds) the
     *        created file should be kept around before it is automatically deleted
     * @return the builder
     */
    public Builder setTTL(long ttl) {
      mTTL = ttl;
      return this;
    }

    /**
     * @param underStorageType the under storage type to use
     * @return the builder
     */
    public Builder setUnderStorageType(UnderStorageType underStorageType) {
      mUnderStorageType = underStorageType;
      return this;
    }

    /**
     * Builds a new instance of {@link CreateOptions}.
     *
     * @return a {@link CreateOptions} instance
     */
    public CreateOptions build() {
      return new CreateOptions(this);
    }
  }

  /**
   * @return the default {@link CreateOptions}
   */
  public static CreateOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final long mBlockSizeBytes;
  private final boolean mRecursive;
  private final long mTTL;
  private final UnderStorageType mUnderStorageType;

  private CreateOptions(CreateOptions.Builder builder) {
    mBlockSizeBytes = builder.mBlockSizeBytes;
    mRecursive = builder.mRecursive;
    mTTL = builder.mTTL;
    mUnderStorageType = builder.mUnderStorageType;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTTL() {
    return mTTL;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  public CreateTOptions toThrift() {
    CreateTOptions options = new CreateTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mUnderStorageType.isPersist());
    options.setRecursive(mRecursive);
    options.setTtl(mTTL);
    return options;
  }
}
