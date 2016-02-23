/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.thrift.CreateFileTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for creating a file.
 */
@NotThreadSafe
public final class CreateFileOptions {
  /**
   * Builder for {@link CreateFileOptions}.
   */
  public static class Builder {
    private long mBlockSizeBytes;
    private long mOperationTimeMs;
    private boolean mPersisted;
    private boolean mRecursive;
    private long mTtl;

    /**
     * Creates a new builder for {@link CreateFileOptions}.
     *
     * @param conf an Alluxio configuration
     */
    public Builder(Configuration conf) {
      mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
      mOperationTimeMs = System.currentTimeMillis();
      mPersisted = false;
      mRecursive = false;
      mTtl = Constants.NO_TTL;
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
     * @param operationTimeMs the operation time to use
     * @return the builder
     */
    public Builder setOperationTimeMs(long operationTimeMs) {
      mOperationTimeMs = operationTimeMs;
      return this;
    }

    /**
     * @param persisted the persisted flag to use; it specifies whether the object to created is
     *        persisted in UFS
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
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
     * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
     *        created file should be kept around before it is automatically deleted
     * @return the builder
     */
    public Builder setTtl(long ttl) {
      mTtl = ttl;
      return this;
    }

    /**
     * Builds a new instance of {@link CreateFileOptions}.
     *
     * @return a {@link CreateFileOptions} instance
     */
    public CreateFileOptions build() {
      return new CreateFileOptions(this);
    }
  }

  private long mBlockSizeBytes;
  private long mOperationTimeMs;
  private boolean mPersisted;
  private boolean mRecursive;
  private long mTtl;

  /**
   * @return the default {@link CreateFileOptions}
   */
  public static CreateFileOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private CreateFileOptions(CreateFileOptions.Builder builder) {
    mBlockSizeBytes = builder.mBlockSizeBytes;
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
    mTtl = builder.mTtl;
  }

  /**
   * Creates a new instance of {@link CreateFileOptions} from {@link CreateFileTOptions}.
   *
   * @param options Thrift options
   */
  public CreateFileOptions(CreateFileTOptions options) {
    mBlockSizeBytes = options.getBlockSizeBytes();
    mOperationTimeMs = System.currentTimeMillis();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTtl = options.getTtl();
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the persisted flag; it specifies whether the object to create is persisted in UFS
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   * they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in milliseconds) the created file
   * should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }
}
