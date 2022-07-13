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

package alluxio.client.file.cache.limiter;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;

/**
 * Interface of write limiter for ssd endurance.
 */
public interface WriteLimiter {

  /**
   * Create a WriteLimiter.
   * @param conf the alluxio configuration
   * @return the write limiter
   */
  static WriteLimiter create(AlluxioConfiguration conf) {
    return CommonUtils.createNewClassInstance(
        conf.getClass(PropertyKey.USER_CLIENT_CACHE_WRITE_LIMITER_CLASS),
        new Class[] {AlluxioConfiguration.class},
        new Object[] {conf});
  }

  /**
   * @param writeLength
   * @return Whether to allow writing
   */
  boolean tryAcquire(int writeLength);
}
