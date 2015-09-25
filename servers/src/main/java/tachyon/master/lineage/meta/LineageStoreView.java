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

import com.google.common.base.Preconditions;

/**
 * This class exposes a readonly view of {@link LineageStore} to checkpoint schedulers and recompute
 * planners.
 *
 * TODO(yupeng): return a view a lineage for protection
 */
public final class LineageStoreView {
  /** The LineageStore this view is derived from */
  private final LineageStore mLineageStore;

  public LineageStoreView(LineageStore lineageStore) {
    mLineageStore = Preconditions.checkNotNull(lineageStore);
  }

  /**
   * Gets all the root lineages in the lineage store.
   */
  public List<Lineage> getRootLineage() {
    return mLineageStore.getRootLineages();
  }

  /**
   * @return the children lineages of a given lineage in the store.
   */
  public List<Lineage> getChildren(Lineage lineage) {
    return mLineageStore.getChildren(lineage);
  }

  /**
   * @return all the lineages in topological order.
   */
  public List<Lineage> getAllLineagesInTopologicalOrder() {
    return mLineageStore.getAllInTopologicalOrder();
  }
}
