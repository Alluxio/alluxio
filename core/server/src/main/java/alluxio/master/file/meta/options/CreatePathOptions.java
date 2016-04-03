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

package alluxio.master.file.meta.options;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.security.authorization.PermissionStatus;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for creating a path.
 */
@NotThreadSafe
public final class CreatePathOptions {

  /**
   * Builder for {@link CreatePathOptions}.
   */
  public static class Builder {
    private boolean mAllowExists;
    private long mBlockSizeBytes;
    private boolean mDirectory;
    private long mOperationTimeMs;
    private boolean mPersisted;
    private boolean mRecursive;
    private long mTtl;
    private PermissionStatus mPermissionStatus;

    /**
     * Creates a new builder for {@link CreatePathOptions}.
     */
    public Builder() {
      this(MasterContext.getConf());
    }

    /**
     * Creates a new builder for {@link CreatePathOptions}.
     *
     * @param conf an Alluxio configuration
     */
    public Builder(Configuration conf) {
      mAllowExists = false;
      mBlockSizeBytes = conf.getBytes(Constants.USER_BLOCK_SIZE_BYTES_DEFAULT);
      mDirectory = false;
      mOperationTimeMs = System.currentTimeMillis();
      mRecursive = false;
      mPersisted = false;
      mTtl = Constants.NO_TTL;
      mPermissionStatus = PermissionStatus.getDirDefault();
    }

    /**
     * @param allowExists the allowExists flag value to use; it specifies whether an exception
     *        should be thrown if the object being made already exists.
     * @return the builder
     */
    public Builder setAllowExists(boolean allowExists) {
      mAllowExists = allowExists;
      return this;
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
     * @param directory the directory flag to use; it specifies whether the object to create is a
     *        directory
     * @return the builder
     */
    public Builder setDirectory(boolean directory) {
      mDirectory = directory;
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
     * @param permissionStatus the permission status to use
     * @return the builder
     */
    public Builder setPermissionStatus(PermissionStatus permissionStatus) {
      mPermissionStatus = permissionStatus;
      return this;
    }

    /**
     * Builds a new instance of {@link CreatePathOptions}.
     *
     * @return a {@code CreateOptions} instance
     */
    public CreatePathOptions build() {
      return new CreatePathOptions(this);
    }
  }

  /**
   * @return the default {@link CreatePathOptions}
   */
  public static CreatePathOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private final boolean mAllowExists;
  private final long mBlockSizeBytes;
  private final boolean mDirectory;
  private final long mOperationTimeMs;
  private final boolean mPersisted;
  private final boolean mRecursive;
  private final long mTtl;
  private PermissionStatus mPermissionStatus;

  private CreatePathOptions(CreatePathOptions.Builder builder) {
    mAllowExists = builder.mAllowExists;
    mBlockSizeBytes = builder.mBlockSizeBytes;
    mDirectory = builder.mDirectory;
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
    mTtl = builder.mTtl;
    mPermissionStatus = builder.mPermissionStatus;
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the object
   *         being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the block size
   */
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  /**
   * @return the directory flag; it specifies whether the object to create is a directory
   */
  public boolean isDirectory() {
    return mDirectory;
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
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the permission status
   */
  public PermissionStatus getPermissionStatus() {
    return mPermissionStatus;
  }
}
