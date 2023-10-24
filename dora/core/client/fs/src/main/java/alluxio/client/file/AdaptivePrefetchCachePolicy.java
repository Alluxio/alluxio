/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

/**
 * An improved implementation of the prefetch cache policy that only halves the prefetch size,
 * on cache miss.
 */
public class AdaptivePrefetchCachePolicy implements PrefetchCachePolicy {
  private long mPrefetchSize = 0;
  private long mLastCallEndPos = -1;
  private final long mMaxPrefetchSize =
      Configuration.getBytes(PropertyKey.USER_POSITION_READER_STREAMING_PREFETCH_MAX_SIZE);

  @Override
  public void addTrace(long pos, int size) {
    if (pos == mLastCallEndPos) {
      mPrefetchSize = Math.min(mMaxPrefetchSize, mPrefetchSize + size);
    }
    mLastCallEndPos = pos + size;
  }

  @Override
  public void onCacheHitRead() {
    // Noop
  }

  @Override
  public void onCacheMissRead() {
    mPrefetchSize /= 2;
  }

  @Override
  public long getPrefetchSize() {
    return mPrefetchSize;
  }
}
