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

package tachyon.master.lineage.checkpoint;

import com.google.common.collect.Lists;

import tachyon.master.lineage.meta.Lineage;
import tachyon.master.lineage.meta.LineageStoreView;

/**
 * This class tries to checkpoint the latest created lineage that is ready for persistence. This
 * class serves as an example to implement an Evictor.
 */
public final class CheckpointLatestScheduler implements CheckpointScheduler {

  /**
   * CheckpointLatestScheduler does not use the lineage store view.
   *
   * @param storeView view of a lineage store.
   */
  public CheckpointLatestScheduler(LineageStoreView storeView) {}

  @Override
  public CheckpointPlan schedule(LineageStoreView store) {
    Lineage toCheckpoint = null;
    long latestCreated = 0;
    for (Lineage lineage : store.getAllLineagesInTopologicalOrder()) {
      if (!lineage.isCompleted() || lineage.isPersisted() || lineage.needRecompute()
          || lineage.isInCheckpointing()) {
        continue;
      }
      if (lineage.getCreationTime() > latestCreated) {
        latestCreated = lineage.getCreationTime();
        toCheckpoint = lineage;
      }
    }

    return toCheckpoint == null ? new CheckpointPlan(Lists.<Long>newArrayList())
        : new CheckpointPlan(Lists.newArrayList(toCheckpoint.getId()));
  }
}
