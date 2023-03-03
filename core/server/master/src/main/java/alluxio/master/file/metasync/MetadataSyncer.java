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

package alluxio.master.file.metasync;

import alluxio.AlluxioURI;
import alluxio.underfs.UfsStatus;

import java.util.List;
import javax.annotation.Nullable;

/**
 * The metadata syncer.
 */
public class MetadataSyncer {
  /**
   * Performs a metadata sync.
   * @param path the path to sync
   * @param isRecursive if children should be loaded recursively
   * @return the metadata sync result
   */
  public SyncResult sync(AlluxioURI path, boolean isRecursive) {
    System.out.println("Syncing...");
    return new SyncResult(false, 0);
  }

  /**
   * Performs a metadata sync asynchronously and return a job id (?).
   */
  public void syncAsync() {}

  // Path loader
  private void loadPaths() {}

  // UFS loader
  private void loadMetadataFromUFS() {}



  private void updateMetadata(@Nullable UfsStatus previousItem, List<UfsStatus> items) {

  }
}
