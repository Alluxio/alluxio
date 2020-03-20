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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.collections.ConcurrentHashSet;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a cache from an Alluxio namespace URI ({@link AlluxioURI}, i.e. /path/to/inode) to
 * UFS statuses.
 *
 * It also allows associating a path with child inodes, so that the statuses for a specific path can
 * be searched for later.
 */
public class UfsStatusCache2 {
  private final ConcurrentHashMap<AlluxioURI, UfsStatus> mStatuses;
  private final ConcurrentHashMap<UfsStatus, Collection<UfsStatus>> mChildren;

  /**
   * Create a new instance of {@link UfsStatusCache2}.
   */
  public UfsStatusCache2() {
    mStatuses = new ConcurrentHashMap<>();
    mChildren = new ConcurrentHashMap<>();
  }

  /**
   * Add a new status to the cache.
   *
   * The last component of the path in the {@link AlluxioURI} must match the result of
   * {@link UfsStatus#getName()}.
   *
   * @param path the Alluxio path to key on
   * @param status the ufs status to store
   * @throws IllegalArgumentException if the status already exists
   */
  public void addStatus(AlluxioURI path, UfsStatus status) {
    UfsStatus prev = mStatuses.putIfAbsent(path, status);
    if (prev != null) {
      throw new IllegalArgumentException(String.format("Cannot add UfsStatus (%s) with Alluxio "
          + "path (%s) that already exists", status, path));
    }
    if (!path.getName().equals(status.getName())) {
      throw new IllegalArgumentException(
          String.format("path name %s does not match ufs status name %s",
              path.getName(), status.getName()));
    }
  }

  /**
   * Add a parent-child mapping to the status cache.
   *
   * All child statuses added via this method will be available via {@link #getStatus(AlluxioURI)}.
   *
   * @param path the directory inode path which contains the children
   * @param children the children of the {@code path}
   * @throws IllegalArgumentException when {@code path} already exists or if any child already
   *                                  exists
   */
  public void addChildren(AlluxioURI path, Collection<UfsStatus> children) {
    UfsStatus status = mStatuses.get(path);
    if (status == null) {
      throw new IllegalArgumentException(
          String.format("UfsStatus at path %s does not exist yet in the cache", path));
    }
    mChildren.computeIfAbsent(status, ufsStatus -> new ConcurrentHashSet<>()).addAll(children);
    children.forEach(child -> {
      AlluxioURI childPath = path.joinUnsafe(child.getName());
      addStatus(childPath, child);
    });
  }

  /**
   * Remove a status from the cache.
   *
   *  Any children added to this status will remain in the cache.
   *
   * @param path the path corresponding to the {@link UfsStatus} to remove
   * @return the removed UfsStatus
   */
  public UfsStatus remove(AlluxioURI path) {
    UfsStatus removed = mStatuses.remove(path);
    if (removed == null) {
      return null;
    }

    mChildren.remove(removed); // ok if there aren't any children
    return removed;
  }

  /**
   * Get the UfsStatus from a given AlluxioURI.
   *
   * @param path the path the retrieve
   * @return The corresponding {@link UfsStatus} or {@code null} if there is none stored
   */
  public UfsStatus getStatus(AlluxioURI path) {
    return mStatuses.get(path);
  }

  /**
   * Get the child {@link UfsStatus}es from a given {@link AlluxioURI}.
   *
   * @param path the path the retrieve
   * @return The corresponding {@link UfsStatus} or {@code null} if there is none stored
   */
  public Collection<UfsStatus> getChildren(AlluxioURI path) {
    UfsStatus stat = getStatus(path);
    if (stat == null) {
      return null;
    }
    return mChildren.get(stat);
  }
}
