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

package tachyon.client.next;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.thrift.NetAddress;

/**
 * Represents the set of operation specific configuration options a user can pass in to a Tachyon
 * client. Not all options will be valid for all operations.
 */
public class ClientOptions {

  public static class Builder {
    private long mBlockSize;
    private CacheType mCacheType;
    private UnderStorageType mUnderStorageType;
    private NetAddress mLocation;

    public Builder(TachyonConf conf) {
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mCacheType = conf.getEnum(Constants.USER_DEFAULT_CACHE_TYPE, CacheType.CACHE);
      mUnderStorageType =
          conf.getEnum(Constants.USER_DEFAULT_UNDER_STORAGE_TYPE, UnderStorageType.NO_PERSIST);
      mLocation = null;
    }

    public Builder setCacheType(CacheType cacheType) {
      mCacheType = cacheType;
      return this;
    }

    public Builder setLocation(NetAddress location) {
      throw new UnsupportedOperationException("Set location is currently unsupported.");
    }

    public Builder setUnderStorageType(UnderStorageType underStorageType) {
      mUnderStorageType = underStorageType;
      return this;
    }

    public Builder setBlockSize(long blockSize) {
      mBlockSize = blockSize;
      return this;
    }

    public ClientOptions build() {
      return new ClientOptions(this);
    }
  }

  private final long mBlockSize;
  private final CacheType mCacheType;
  private final UnderStorageType mUnderStorageType;
  private final NetAddress mLocation;

  // TODO: Add a constructor that just uses defaults
  private ClientOptions(ClientOptions.Builder builder) {
    mBlockSize = builder.mBlockSize;
    mCacheType = builder.mCacheType;
    mUnderStorageType = builder.mUnderStorageType;
    mLocation = builder.mLocation;
  }

  public long getBlockSize() {
    return mBlockSize;
  }

  public CacheType getCacheType() {
    return mCacheType;
  }

  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  public NetAddress getLocation() {
    return mLocation;
  }
}
