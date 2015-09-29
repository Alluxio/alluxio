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

public class CreatePathOptions {
  public static class Builder {
    private long mBlockSize;
    private boolean mDirectory;
    private long mOperationTime;
    private boolean mPersisted;
    private boolean mRecursive;
    private long mTTL;

    /**
     * Creates a new builder for {@link CreatePathOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mDirectory = false;
      mOperationTime = System.currentTimeMillis();
      mRecursive = false;
      mPersisted = false;
      mTTL = Constants.NO_TTL;
    }

    /**
     * @param blockSize the block size to use
     * @return the builder
     */
    public Builder setBlockSize(long blockSize) {
      mBlockSize = blockSize;
      return this;
    }

    /**
     * @param directory TODO
     * @return the builder
     */
    public Builder setDirectory(boolean directory) {
      mDirectory = directory;
      return this;
    }

    /**
     * @param operationTime TODO
     * @return the builder
     */
    public Builder setOperationTime(long operationTime) {
      mOperationTime = operationTime;
      return this;
    }

    /**
     * @param persisted TODO
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
      return this;
    }

    /**
     * @param recursive the recursive flag value to use; it specifies whether parent directories
     *                  should be created if they do not already exist
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * @param ttl the TTL (time to live) value to use; it identifies duration (in seconds) the
     *            created file should be kept around before it is automatically deleted
     * @return the builder
     */
    public Builder setTTL(long ttl) {
      mTTL = ttl;
      return this;
    }

    /**
     * Builds a new instance of {@code CreateOptions}.
     *
     * @return a {@code CreateOptions} instance
     */
    public CreatePathOptions build() {
      return new CreatePathOptions(this);
    }
  }

  /**
   * @return the default {@code CreateOptions}
   */
  public static CreatePathOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private final long mBlockSize;
  private final boolean mDirectory;
  private final long mOperationTime;
  private final boolean mPersisted;
  private final boolean mRecursive;
  private final long mTTL;

  private CreatePathOptions(CreatePathOptions.Builder builder) {
    mBlockSize = builder.mBlockSize;
    mDirectory = builder.mDirectory;
    mOperationTime = builder.mOperationTime;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
    mTTL = builder.mTTL;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return TODO
   */
  public boolean isDirectory() {
    return mDirectory;
  }

  /**
   * @return TODO
   */
  public long getOperationTime() {
    return mOperationTime;
  }

  /**
   * @return TODO
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
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   * should be kept around before it is automatically deleted
   */
  public long getTTL() {
    return mTTL;
  }
}
