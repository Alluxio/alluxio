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

package alluxio.worker.block;

import alluxio.StorageTierAssoc;
import alluxio.collections.Pair;

import java.util.List;
import java.util.Map;

/**
 * Interface for the block store meta in Alluxio.
 */
public interface BlockStoreMeta {
  /**
   * Note: This method is only available when users initialize
   * the blockStoreMeta with full block meta requirement.
   *
   * @return A mapping from storage tier alias to blocks
   */
  Map<String, List<Long>> getBlockList();

  /**
   * Note: This method is only available when users initialize
   * the blockStoreMeta with full block meta requirement.
   *
   * @return A mapping from storage location alias to blocks
   */
  Map<BlockStoreLocation, List<Long>> getBlockListByStorageLocation();

  /**
   * @return the capacity in bytes
   */
  long getCapacityBytes();

  /**
   * @return a mapping from tier aliases to capacity in bytes
   */
  Map<String, Long> getCapacityBytesOnTiers();

  /**
   * @return a mapping from tier directory-path pairs to capacity in bytes
   */
  Map<Pair<String, String>, Long> getCapacityBytesOnDirs();

  /**
   * @return a mapping from tier aliases to directory paths in that tier
   */
  Map<String, List<String>> getDirectoryPathsOnTiers();

  /**
   * @return a mapping from tier alias to lost storage paths
   */
  Map<String, List<String>> getLostStorage();

  /**
   * Note: This is only available when blocks are included.
   *
   * @return the number of blocks
   */
  int getNumberOfBlocks();

  /**
   * @return the used capacity in bytes
   */
  long getUsedBytes();

  /**
   * @return a mapping from tier aliases to used capacity in bytes
   */
  Map<String, Long> getUsedBytesOnTiers();

  /**
   * @return a mapping from tier directory-path pairs to used capacity in bytes
   */
  Map<Pair<String, String>, Long> getUsedBytesOnDirs();

  /**
   * @return the storage tier mapping
   */
  StorageTierAssoc getStorageTierAssoc();
}
