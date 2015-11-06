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

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.master.lineage.meta.LineageStoreView;
import tachyon.util.CommonUtils;

/**
 * Scheduling strategy for Lineage checkpointing.
 */
public interface CheckpointScheduler {

  class Factory {
    /**
     * @param conf TachyonConf to determine the scheduler type
     * @return the generated scheduler
     */
    public static CheckpointScheduler createScheduler(TachyonConf conf, LineageStoreView store) {
      try {
        return CommonUtils.createNewClassInstance(
            conf.<CheckpointScheduler>getClass(Constants.MASTER_LINEAGE_CHECKPOINT_CLASS),
            new Class[] {LineageStoreView.class}, new Object[] {store});
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Generates a plan to decide what lineages to checkpoint.
   *
   * <p>
   * This method returns null if the scheduler fails to propose a feasible plan to find the lineages
   * to checkpoint. If the checkpoint plan has no lineages, it indicates that the scheduler has no
   * actions to take and the requirement is already met.
   * </p>
   *
   * @param store a readonly view of the lineage store
   * @return a scheduling plan (possibly empty) to checkpoint the lineages, or null if no plan is
   *         feasible
   */
  CheckpointPlan schedule(LineageStoreView store);
}
