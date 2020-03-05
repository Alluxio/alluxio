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

package alluxio.worker.block.annotator;

import alluxio.annotation.PublicApi;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.CommonUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;

/**
 * Interface for providers that annotates blocks for sorting.
 *
 * @param <T> sorted-field type for the annotator
 */
@PublicApi
public interface BlockAnnotator<T extends BlockSortedField> {

  /**
   * Factory for {@link BlockAnnotator}.
   */
  @ThreadSafe
  class Factory {
    private Factory() {} // prevent instantiation

    /**
     * Creates {@link BlockAnnotator} implementation based
     * on Alluxio configuration.
     *
     * @return the generated {@link BlockAnnotator} instance
     */
    public static BlockAnnotator create() {
      return CommonUtils.createNewClassInstance(
          ServerConfiguration.getClass(PropertyKey.WORKER_BLOCK_ANNOTATOR_CLASS), null,
          null);
    }
  }

  /**
   * Used to get a new sorted-field for the given block
   * at the current logical time.
   *
   * @param blockId block Id
   * @param oldValue old sorted-field value
   * @return the new sorted-field value
   */
  BlockSortedField updateSortedField(long blockId, T oldValue);

  /**
   * Updates sorted-field values for all {block-id, sorted-field} pairs
   * at the same logical time.
   * Note: Currently not required for online schemes, so not called.
   *
   * @param blockList list of {block-id, sorted-field} pairs
   */
  void updateSortedFields(List<Pair<Long, T>> blockList);

  /**
   * Used to report whether the block annotator is an online sorter.
   *
   * For offline sorters, {@link #updateSortedFields(List)} will be called before
   * acquiring an iterator for a particular location.
   *
   * @return {@code true} if an online sorter
   */
  boolean isOnlineSorter();
}
