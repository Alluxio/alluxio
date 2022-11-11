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

package alluxio.qos;

import java.util.concurrent.TimeUnit;

/**
 * Rate Limiter Interface.
 */
public interface RateLimiter {
  /**
   * A blocked method to acquire resources.
   * @param permits num of requested resource
   */
  public void acquire(int permits);

  /**
   * An un-blocked method to acquire resources.
   * @param permits num of requested resource
   * @return true if resource is acquired otherwise false
   */
  public boolean tryAcquire(int permits);

  /**
   * An un-blocked method to acquire resources.
   * @param permits num of requested resource
   * @param timeOut try to acquire resources until times up
   * @param timeUnit time unit of timeout
   * @return true if resource is acquired otherwise false
   */
  public boolean tryAcquire(int permits, long timeOut, TimeUnit timeUnit);
}
