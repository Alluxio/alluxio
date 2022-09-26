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

package alluxio.client.file;

/**
 * The pending Path in the Inode SyncStream traversal order.
 * For general-scale directory, BFS can archive better sync speed. For super-large-scale
 * directory (> 100_000_000), BFS may cause a large amount of memory to be used for
 * ufsStatusCache. DFS can ensure a smaller ufsStatusCache size to make the memory
 * smaller and the gc time shorter.
 *
 * If the size of ufsStatusCache is still large, consider setting
 * PropertyKey.MASTER_METADATA_SYNC_PREFETCH_CHILDREN_UFS_STATUS to false.
 */
public enum MetadataSyncTraversalOrder {
  BFS,
  DFS
}
