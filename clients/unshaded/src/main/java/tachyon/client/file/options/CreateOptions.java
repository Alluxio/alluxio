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
import tachyon.thrift.CreateTOptions;

/**
 * Method option for creating a file.
 */
@PublicApi
public final class CreateOptions {

  /**
   * Builder for {@link CreateOptions}.
   */
  public static class Builder implements OptionsBuilder<CreateOptions> {
    // TODO(calvin): Should this just be an int?
    private long mBlockSizeBytes;
    private boolean mRecursive;
    private long mTtl;
    private UnderStorageType mUnderStorageType;

    /**
     * Creates a new builder for {@link CreateOptions}.
     */
    public Builder() {
      this(ClientContext.getConf());
    }

    /**
     * Creates a new builder for {@link CreateOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
      mRecursive = false;
      mTtl = Constants.NO_TTL;
      WriteType defaultWriteType =
          conf.getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
      mUnderStorageType = defaultWriteType.getUnderStorageType();
    }

    /**
     * Sets the size of the block in bytes.
     *
     * @param blockSizeBytes the block size to use
     * @return the builder
     */
    public Builder setBlockSizeBytes(long blockSizeBytes) {
      mBlockSizeBytes = blockSizeBytes;
      return this;
    }

    /**
     * Sets the recursive flag.
     *
     * @param recursive the recursive flag value to use; it specifies whether parent directories
     *        should be created if they do not already exist
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Sets the time to live.
     *
     * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
     *        created file should be kept around before it is automatically deleted, irrespective of
     *        whether the file is pinned
     * @return the builder
     */
    public Builder setTtl(long ttl) {
      mTtl = ttl;
      return this;
    }

    /**
     * This is an advanced API, use {@link Builder#setWriteType(WriteType)} when possible.
     *
     * @param underStorageType the under storage type to use
     * @return the builder
     */
    public Builder setUnderStorageType(UnderStorageType underStorageType) {
      mUnderStorageType = underStorageType;
      return this;
    }

    /**
     * Sets the write type.
     *
     * @param writeType the write type to use
     * @return the builder
     */
    public Builder setWriteType(WriteType writeType) {
      mUnderStorageType = writeType.getUnderStorageType();
      return this;
    }

    /**
     * Builds a new instance of {@link CreateOptions}.
     *
     * @return a {@link CreateOptions} instance
     */
    @Override
    public CreateOptions build() {
      return new CreateOptions(this);
    }
  }

  /**
   * @return the default {@link CreateOptions}
   */
  public static CreateOptions defaults() {
    return new Builder().build();
  }

  private final long mBlockSizeBytes;
  private final boolean mRecursive;
  private final long mTtl;
  private final UnderStorageType mUnderStorageType;

  private CreateOptions(CreateOptions.Builder builder) {
    mBlockSizeBytes = builder.mBlockSizeBytes;
    mRecursive = builder.mRecursive;
    mTtl = builder.mTtl;
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
   * @return the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *         created file should be kept around before it is automatically deleted, irrespective of
   *         whether the file is pinned
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("CreateOptions(");
    sb.append(super.toString()).append(", BlockSizeBytes: ").append(mBlockSizeBytes);
    sb.append(", Recursive: ").append(mRecursive);
    sb.append(", TTL: ").append(mTtl);
    sb.append(", UnderStorageType: ").append(mUnderStorageType.toString());
    sb.append(")");
    return sb.toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateTOptions toThrift() {
    CreateTOptions options = new CreateTOptions();
    options.setBlockSizeBytes(mBlockSizeBytes);
    options.setPersisted(mUnderStorageType.isSyncPersist());
    options.setRecursive(mRecursive);
    options.setTtl(mTtl);
    return options;
  }
}
