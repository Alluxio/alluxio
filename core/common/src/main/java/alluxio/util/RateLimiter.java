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

package alluxio.util;

import java.util.Optional;

/**
 * Used to limit the rate of operations. This rate limiter is not thread safe
 * and the operations are non-blocking. It is used by acquiring a permit for
 * each operation, then checking how the operation should wait by calling
 * {@link RateLimiter#getWaitTimeNanos(long)}.
 */
public interface RateLimiter {

  /**
   * Acquire a permit for the next operation.
   * @return {@link Optional#empty()} if no waiting is needed, otherwise
   * the value contained in the returned optional is the permit, which
   * can be used in calls to {@link RateLimiter#getWaitTimeNanos}
   * to see how long to wait for the operation to be ready.
   */
  Optional<Long> acquire();

  /**
   * Checks how long is needed to wait for this permit to be ready.
   * @param permit the permit returned by {@link RateLimiter#acquire()}
   * @return the amount of time needed to wait in nanoseconds
   */
  long getWaitTimeNanos(long permit);

  /**
   * @param permitsPerSecond permits per second
   * @return a rate limiter
   */
  static RateLimiter createRateLimiter(long permitsPerSecond) {
    if (permitsPerSecond <= 0) {
      return new RateLimiter() {
        @Override
        public Optional<Long> acquire() {
          return Optional.empty();
        }

        @Override
        public long getWaitTimeNanos(long permit) {
          return 0;
        }
      };
    }
    return new SimpleRateLimiter(permitsPerSecond);
  }
}
