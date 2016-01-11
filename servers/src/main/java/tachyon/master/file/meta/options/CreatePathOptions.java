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

package tachyon.master.file.meta.options;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.MasterContext;
import tachyon.security.authorization.PermissionStatus;

/**
 * Method option for creating a path.
 */
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
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
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
     * Builds a new instance of {@link tachyon.master.file.options.CreateOptions}.
     *
     * @return a {@code CreateOptions} instance
     */
    public CreatePathOptions build() {
      return new CreatePathOptions(this);
    }
  }

  /**
   * @return the default {@link tachyon.master.file.options.CreateOptions}
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
