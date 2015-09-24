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

package tachyon.client.options;

import tachyon.Constants;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.conf.TachyonConf;

public class OutStreamOptions {
  public static class Builder {
    private long mBlockSize;
    private TachyonStorageType mTachyonStorageType;
    private UnderStorageType mUnderStorageType;
    private String mHostname;

    public Builder(TachyonConf conf) {
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mTachyonStorageType =
          conf.getEnum(Constants.USER_DEFAULT_TACHYON_STORAGE_TYPE, TachyonStorageType.class);
      mUnderStorageType =
          conf.getEnum(Constants.USER_DEFAULT_UNDER_STORAGE_TYPE, UnderStorageType.class);
      mHostname = null;
    }

    /**
     * @param hostname the hostname to use
     * @return the builder
     */
    public Builder setHostname(String hostname) {
      mHostname = hostname;
      return this;
    }

    /**
     * @param tachyonStorageType the Tachyon storage type to use
     * @return the builder
     */
    public Builder setTachyonStorageType(TachyonStorageType tachyonStorageType) {
      mTachyonStorageType = tachyonStorageType;
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
     * @param blockSize the block size to use
     * @return the builder
     */
    public Builder setBlockSize(long blockSize) {
      mBlockSize = blockSize;
      return this;
    }

    /**
     * Builds a new instance of <code>ClientOptions</code>
     *
     * @return a <code>ClientOptions</code> instance
     */
    public OutStreamOptions build() {
      return new OutStreamOptions(this);
    }
  }

  private final long mBlockSize;
  private final TachyonStorageType mTachyonStorageType;
  private final UnderStorageType mUnderStorageType;
  private final String mHostname;

  /**
   * @return the default <code>ClientOptions</code>
   */
  public static OutStreamOptions defaults() {
    return new Builder(new TachyonConf()).build();
  }

  private OutStreamOptions(OutStreamOptions.Builder builder) {
    mBlockSize = builder.mBlockSize;
    mTachyonStorageType = builder.mTachyonStorageType;
    mUnderStorageType = builder.mUnderStorageType;
    mHostname = builder.mHostname;
  }

  /**
   * @return the block size
   */
  public long getBlockSize() {
    return mBlockSize;
  }

  /**
   * @return the cache type
   */
  public TachyonStorageType getTachyonStorageType() {
    return mTachyonStorageType;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  /**
   * @return the hostname
   */
  public String getHostname() {
    return mHostname;
  }

  public InStreamOptions toInStreamOptions() {
    return new InStreamOptions.Builder(new TachyonConf())
        .setTachyonStorageType(mTachyonStorageType).build();
  }
}
