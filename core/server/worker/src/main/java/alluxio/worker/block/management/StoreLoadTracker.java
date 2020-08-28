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

package alluxio.worker.block.management;

import alluxio.worker.block.BlockStoreLocation;

/**
 * Interface for detecting user activity on worker locations.
 */
public interface StoreLoadTracker {
  /**
   * Used to detect load on particular location.
   *
   * @param locations vararg of locations
   * @return {@code true} if load detected on any given location
   */
  boolean loadDetected(BlockStoreLocation... locations);
}
