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

package alluxio.resource;

import java.util.concurrent.TimeUnit;

/**
 * Interface representing a pool of resources to be temporarily used and returned.
 *
 * @param <T> the type of resource this pool manages
 */
public interface Pool<T> {
  /**
   * Acquires a resource from the pool.
   *
   * @return the acquired resource which should not be null
   * @throws Exception if it fails
   */
  T acquire() throws Exception;

  /**
   * Acquires a resource from the pool.
   *
   * @param time time it takes before timeout if no resource is available
   * @param unit the unit of the time
   * @return the acquired resource which should not be null
   * @throws Exception if it fails
   */
  T acquire(long time, TimeUnit unit) throws Exception;

  /**
   * Releases the resource to the pool.
   *
   * @param resource the resource to release
   */
  void release(T resource);

  /**
   * Closes the pool which clears all the resources.
   */
  void close();

  /**
   * @return the current pool size
   */
  int size();
}
