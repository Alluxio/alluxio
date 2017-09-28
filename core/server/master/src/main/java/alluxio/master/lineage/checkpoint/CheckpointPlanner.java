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

package alluxio.master.lineage.checkpoint;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.lineage.meta.LineageStoreView;
import alluxio.util.CommonUtils;

/**
 * Generates plans for lineage checkpointing.
 */
public interface CheckpointPlanner {

  /**
   * Factory for {@link CheckpointPlanner}.
   */
  class Factory {

    private Factory() {} // prevent instantiation

    /**
     * @param lineageStoreView a view of the lineage store
     * @param fileSystemMasterView a view of the file system master
     * @return the generated planner
     */
    public static CheckpointPlanner create(LineageStoreView lineageStoreView,
        FileSystemMasterView fileSystemMasterView) {
      try {
        return CommonUtils.createNewClassInstance(
            Configuration.<CheckpointPlanner>getClass(PropertyKey.MASTER_LINEAGE_CHECKPOINT_CLASS),
            new Class[] {LineageStoreView.class, FileSystemMasterView.class},
            new Object[] {lineageStoreView, fileSystemMasterView});
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Generates a plan to decide what lineages to checkpoint.
   *
   * <p>
   * This method returns null if the planner fails to propose a feasible plan to find the lineages
   * to checkpoint. If the checkpoint plan has no lineages, it indicates that the planner has no
   * actions to take and the requirement is already met.
   * </p>
   *
   * @param lineageStoreView a readonly view of the lineage store
   * @param fileSystemMasterView a readonly view of the file system master
   * @return a plan (possibly empty) to checkpoint the lineages, or null if no plan is feasible
   */
  CheckpointPlan generatePlan(LineageStoreView lineageStoreView,
      FileSystemMasterView fileSystemMasterView);
}
