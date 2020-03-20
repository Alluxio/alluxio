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

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A class to hold a set of statuses which map from AlluxioURI to UfsStatus.
 */
public class UfsStatusCache {
  private final ConcurrentHashMap<AlluxioURI, UfsStatus> mStatuses;
  private final AlluxioURI mUfsUri;

  /**
   * Creates a new instance of {@link UfsStatusCache}.
   *
   * @param ufsUri the UFS URI used to create the statuses map
   * @param statuses the mapping from {@link AlluxioURI} to {@link UfsStatus}
   */
  public UfsStatusCache(AlluxioURI ufsUri, ConcurrentHashMap<AlluxioURI, UfsStatus> statuses) {
    mStatuses = statuses;
    mUfsUri = ufsUri;
  }

  /**
   * Creates a new instance of {@link UfsStatusCache}.
   *
   * @param ufsUri the UFS URI used to create the statuses map
   * @param statuses the mapping from {@link AlluxioURI} to {@link UfsStatus}
   */
  public UfsStatusCache(AlluxioURI ufsUri, Map<AlluxioURI, UfsStatus> statuses) {
    mStatuses = new ConcurrentHashMap<>(statuses);
    mUfsUri = ufsUri;
  }

  /**
   * Creates a new instance of {@link UfsStatusCache}.
   *
   * @param ufsUri the UFS URI used to create the statuses map
   */
  public UfsStatusCache(AlluxioURI ufsUri) {
    mStatuses = new ConcurrentHashMap<>();
    mUfsUri = ufsUri;
  }

  /**
   * Look up a UFS status based on the Alluxio URI.
   *
   * @param key the {@link AlluxioURI} to lookup
   * @return the UfsStatus if it exists in the cache, otherwise null
   */
  @Nullable
  public UfsStatus get(AlluxioURI key) {
    return mStatuses.get(key);
  }

  /**
   * Remove a status from the status cache.
   *
   * @param key the {@link AlluxioURI} for the status
   * @return the status that the cache stored, or none
   */
  public UfsStatus remove(AlluxioURI key) {
    return mStatuses.remove(key);
  }

  /**
   * @return a collection of all {@link UfsStatus} in the cache
   */
  public Collection<UfsStatus> values() {
    return mStatuses.values();
  }

  /**
   * @return the UFS URI for this status cache
   */
  public AlluxioURI getUfsUri() {
    return mUfsUri;
  }

  /**
   * @return true if the cache is empty
   */
  public boolean isEmpty() {
    return mStatuses.isEmpty();
  }
}
