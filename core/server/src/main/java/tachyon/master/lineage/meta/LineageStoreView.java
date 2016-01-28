/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.lineage.meta;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;

import tachyon.exception.LineageDoesNotExistException;

/**
 * This class exposes a read-only view of {@link LineageStore} to checkpoint schedulers and
 * recompute planners.
 *
 * TODO(yupeng): return a lineage view for protection
 */
@ThreadSafe
public final class LineageStoreView {
  /** The LineageStore this view is derived from */
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
   * @param lineage the lineage to get the childrens from
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
