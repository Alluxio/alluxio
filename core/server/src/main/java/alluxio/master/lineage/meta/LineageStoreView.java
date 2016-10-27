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

package alluxio.master.lineage.meta;

import alluxio.exception.LineageDoesNotExistException;

import com.google.common.base.Preconditions;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class exposes a read-only view of {@link LineageStore} to checkpoint schedulers and
 * recompute planners.
 *
 * TODO(yupeng): return a lineage view for protection
 */
@ThreadSafe
public final class LineageStoreView {
  /** The LineageStore this view is derived from. */
  private final LineageStore mLineageStore;

  /**
   * Creates a new instance of {@link LineageStoreView}.
   *
   * @param lineageStore the underlying lineage store to construct the view for
   */
  public LineageStoreView(LineageStore lineageStore) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
  }

  /**
   * @return the list of all root lineages
   */
  public List<Lineage> getRootLineage() {
    return mLineageStore.getRootLineages();
  }

  /**
   * @param lineage the lineage to get the children from
   * @return the children lineages of a given lineage in the store
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public List<Lineage> getChildren(Lineage lineage) throws LineageDoesNotExistException {
    return mLineageStore.getChildren(lineage);
  }

  /**
   * @return all the lineages in topological order
   */
  public List<Lineage> getAllLineagesInTopologicalOrder() {
    return mLineageStore.getAllInTopologicalOrder();
  }
}
