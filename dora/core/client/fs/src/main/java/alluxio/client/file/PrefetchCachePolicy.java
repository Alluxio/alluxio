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

import com.amazonaws.annotation.NotThreadSafe;

/**
 * The prefetch cache policy to determine the prefetch size.
 */
@NotThreadSafe
public interface PrefetchCachePolicy {
  /**
   * Adds the trace of a read request.
   * @param pos the position
   * @param size the size
   */
  void addTrace(long pos, int size);

  /**
   * Called when a read hits the cache.
   */
  void onCacheHitRead();

  /**
   * Called when a read does not hit the cache.
   */
  void onCacheMissRead();

  /**
   * @return the expected prefetch size
   */
  int getPrefetchSize();

  /**
   * The factory class.
   */
  class Factory {
    /**
     * @return a prefetch cache policy
     */
    public static PrefetchCachePolicy create() {
      if (Configuration.getBoolean(
          PropertyKey.USER_POSITION_READER_STREAMING_ADAPTIVE_POLICY_ENABLED)) {
        return new AdaptivePrefetchCachePolicy();
      }
      return new BasePrefetchCachePolicy();
    }
  }
}
