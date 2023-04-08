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

package alluxio.file.options;

/**
 * Defines how directories will be listed on the UFS when performing
 * metadata synchronization. Only effects recursive listings.
 */
public enum DirectoryLoadType {
  /**
   * Load the path recursively by running a single command which returns results
   * in batches if supported by the UFS. For example on an object store, this
   * will perform a ListBucket operation with no delimiter. This will create
   * less load on the UFS than {@link DirectoryLoadType#BFS} and {@link DirectoryLoadType#DFS}
   * but will be more impacted by latency between Alluxio and the UFS as there
   * is only a single listing running.
   */
  SINGLE_LISTING,
  /**
   * Load the path recursively by loading each nested directory in a separate
   * load command in a breadth first manner. Each directory will be listed in batches
   * if supported by the UFS. Listings of different directories will run concurrently.
   */
  BFS,
  /**
   * Load the path recursively by loading each nested directory in a separate
   * load command in a breadth first manner. Each directory will be listed in batches
   * if supported by the UFS. Listings of different directories will run concurrently.
   */
  DFS
}
