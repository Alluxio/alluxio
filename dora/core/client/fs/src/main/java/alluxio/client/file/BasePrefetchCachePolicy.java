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

import com.google.common.collect.EvictingQueue;

/**
 * A base prefetch cache policy that increases the prefetch window if the read pattern is
 * contiguous and reduce the window down to the read size if it is not.
 */
public class BasePrefetchCachePolicy implements PrefetchCachePolicy {
  private int mPrefetchSize = 0;
  private final EvictingQueue<CallTrace> mCallHistory = EvictingQueue.create(
      Configuration.getInt(PropertyKey.USER_POSITION_READER_STREAMING_MULTIPLIER));

  @Override
  public void addTrace(long pos, int size) {
    mCallHistory.add(new CallTrace(pos, size));
    int consecutiveReadLength = 0;
    long lastReadEnd = -1;
    for (CallTrace trace : mCallHistory) {
      if (trace.mPosition == lastReadEnd) {
        lastReadEnd += trace.mLength;
        consecutiveReadLength += trace.mLength;
      } else {
        lastReadEnd = trace.mPosition + trace.mLength;
        consecutiveReadLength = trace.mLength;
      }
    }
    mPrefetchSize = consecutiveReadLength;
  }

  @Override
  public void onCacheHitRead() {
    // Noop
  }

  @Override
  public void onCacheMissRead() {
    // Noop
  }

  @Override
  public int getPrefetchSize() {
    return mPrefetchSize;
  }

  private static class CallTrace {
    final long mPosition;
    final int mLength;

    private CallTrace(long pos, int length) {
      mPosition = pos;
      mLength = length;
    }
  }
}
