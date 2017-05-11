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

package alluxio.master.file.meta;

import alluxio.AlluxioURI;
import alluxio.exception.InvalidPathException;

/**
 * Cache for recording information about paths that are not present in UFS.
 */
public interface UfsAbsentPathCache {
  /**
   * Removes the cache for the given mount id.
   *
   * @param mountId the mount id
   */
  void removeMountPoint(long mountId);

  /**
   * Adds the given absent path into the cache. This will sequentially walk down the path to find
   * the first component which does not exist in the ufs.
   *
   * @param path the absent path to add to the cache
   * @throws InvalidPathException if the path is invalid
   */
  void addAbsentPath(AlluxioURI path) throws InvalidPathException;

  /**
   * Removes an absent path from the cache.
   *
   * @param path the path to remove from the cache
   * @throws InvalidPathException if the path is invalid
   */
  void removeAbsentPath(AlluxioURI path) throws InvalidPathException;

  /**
   * Returns true if the given path is absent, according to this cache. A path is absent if one of
   * its ancestors is absent.
   *
   * @param path the path to check
   * @return true if the path is absent according to the cache
   * @throws InvalidPathException if the path is invalid
   */
  boolean isAbsent(AlluxioURI path) throws InvalidPathException;
}
