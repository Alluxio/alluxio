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

package alluxio;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to represent what the active syncing process should sync.
 */
public class SyncInfo {
  private static final SyncInfo EMPTY_INFO = new SyncInfo(Collections.emptyMap());

  // A map mapping syncpoints to files changed in those sync points
  private final Map<AlluxioURI, Set<AlluxioURI>> mChangedFilesMap;

  /**
   * Construct a SyncInfo.
   *
   * @param changedFiles changedFile Map
   */
  public SyncInfo(Map<AlluxioURI, Set<AlluxioURI>> changedFiles) {
    mChangedFilesMap = changedFiles;
  }

  /**
   * Returns an empty SyncInfo object.
   *
   * @return emptyInfo object
   */
  public static SyncInfo emptyInfo() {
    return EMPTY_INFO;
  }

  /**
   * Returns a list of sync points.
   *
   * @return a list of sync points
   */
  public Set<AlluxioURI> getSyncPoints() {
    return mChangedFilesMap.keySet();
  }

  /**
   * REturns a set of changed files.
   *
   * @param syncPoint the syncPoint that we are monitoring
   * @return a set of sync points
   */
  public Set<AlluxioURI> getChangedFiles(AlluxioURI syncPoint) {
    return mChangedFilesMap.get(syncPoint);
  }
}
