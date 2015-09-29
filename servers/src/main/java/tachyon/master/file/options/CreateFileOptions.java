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

package tachyon.master.file.options;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.CreateFileTOptions;

public final class CreateFileOptions {
  public static class Builder {
    private long mBlockSize;
    private long mOperationTime;
    private boolean mPersisted;
    private boolean mRecursive;
    private long mTTL;

    /**
     * Creates a new builder for {@link CreateFileOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mOperationTime = System.currentTimeMillis();
      mPersisted = false;
      mRecursive = false;
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
     * @param persisted TODO
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
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
     * Builds a new instance of {@link CreateFileOptions}.
     *
     * @return a {@link CreateFileOptions} instance
     */
    public CreateFileOptions build() {
      return new CreateFileOptions(this);
    }
  }

  private long mBlockSize;
  private long mOperationTime;
  private boolean mPersisted;
  private boolean mRecursive;
  private long mTTL;

  private CreateFileOptions(CreateFileOptions.Builder builder) {
    mBlockSize = builder.mBlockSize;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
    mTTL = builder.mTTL;
  }

  /**
   * Creates a new instance of {@link CreateFileOptions} from {@link CreateFileOptions}.
   *
   * @param options Thrift options
   * @return a {@link CreateFileOptions} instance
   */
  public CreateFileOptions(CreateFileTOptions options) {
    mBlockSize = options.getBlockSize();
    mOperationTime = System.currentTimeMillis();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
    mTTL = options.getTtl();
  }

  /**
   * TODO
   *
   * @return
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return TODO
   */
  public long getOperationTime() {
    return mOperationTime;
  }

  /**
   * TODO
   *
   * @return
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * TODO
   *
   * @return
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * TODO
   *
   * @return
   */
  public long getTTL() {
    return mTTL;
  }
}
