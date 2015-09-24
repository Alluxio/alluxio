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
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;

@PublicApi
public final class CreateOptions {
  public static class Builder {
    private long mBlockSize;
    private boolean mRecursive;
    private long mTTL;

    public Builder(TachyonConf conf) {
      mBlockSize = conf.getBytes(Constants.USER_DEFAULT_BLOCK_SIZE_BYTE);
      mRecursive = false;
    }

    public Builder setBlockSize(long blockSize) {
      mBlockSize = blockSize;
      return this;
    }

    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    public Builder setTTL(long ttl) {
      mTTL = ttl;
      return this;
    }

    public CreateOptions build() {
      return new CreateOptions(this);
    }
  }

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

  public long getBlockSize() {
    return mBlockSize;
  }

  public boolean isRecursive() {
    return mRecursive;
  }

  public long getTTL() {
    return mTTL;
  }
}
