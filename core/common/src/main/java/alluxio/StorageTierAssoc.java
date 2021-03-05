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

import alluxio.collections.Pair;
import alluxio.worker.block.BlockStoreLocation;

import java.util.List;

/**
 * A two-way mapping between storage tier aliases and ordinal numbers from configuration.
 */
public interface StorageTierAssoc {
  /**
   * @param ordinal a storage tier ordinal
   * @return the storage tier alias matching the given ordinal
   */
  String getAlias(int ordinal);

  /**
   * @param alias a storage tier alias
   * @return the storage tier ordinal matching the given alias
   */
  int getOrdinal(String alias);

  /**
   * @return the size of the alias-ordinal mapping
   */
  int size();

  /**
   * @return a list of storage tier aliases in order of their ordinal value
   */
  List<String> getOrderedStorageAliases();

  /**
   * @return list of intersections between tier levels
   */
  List<Pair<BlockStoreLocation, BlockStoreLocation>> intersectionList();
}
