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
import tachyon.conf.TachyonConf;

@PublicApi
public final class CreateOptions {
  public static class Builder {
    // TODO(calvin): Should this just be an int?
    private long mBlockSize;
    private boolean mRecursive;
    private long mTTL;

    /**
     * Creates a new builder for {@link CreateOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mTTL = 0;
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mRecursive = false;
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
     * Builds a new instance of {@code CreateOptions}.
     *
     * @return a {@code CreateOptions} instance
     */
    public CreateOptions build() {
      return new CreateOptions(this);
    }
  }

  /**
   * @return the default {@code CreateOptions}
   */
  public static CreateOptions defaults() {
    return new Builder(ClientContext.getConf()).build();
  }

  private final long mBlockSize;
  private final boolean mRecursive;
  private final long mTTL;

  private CreateOptions(CreateOptions.Builder builder) {
    mBlockSize = builder.mBlockSize;
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
}
