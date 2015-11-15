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

package tachyon.retry;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Utils for working with caches.
 */
public final class CacheUtils {
  // TODO(andrew) Make these configurable
  private static final long DEFAULT_MAX_SIZE = 10000;
  private static final long DEFAULT_EXPIRE_MS = 2000;

  /**
   * @return a cache with the default maximum size and expiration time
   */
  public static <T> Cache<String, T> createCache() {
    return createCache(DEFAULT_MAX_SIZE, DEFAULT_EXPIRE_MS);
  }

  /**
   * @param maxSize the maximum number of elements the cache may hold
   * @param expireMs the amount of time to hold entries before they expire
   * @return a cache with the specified maximum size and expiration time
   */
  public static <T> Cache<String, T> createCache(long maxSize, long expireMs) {
    // TODO(andrew) another constructor which uses CacheBuilder<Object, Object> from(String spec)
    return CacheBuilder.newBuilder().maximumSize(maxSize)
        .expireAfterWrite(expireMs, TimeUnit.MILLISECONDS).<String, T>build();
  }

  private CacheUtils() {} // prevent instantiation
}
