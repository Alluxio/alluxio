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

import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.lineage.meta.Lineage;
import alluxio.master.lineage.meta.LineageStateUtils;
import alluxio.master.lineage.meta.LineageStoreView;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class tries to checkpoint the latest created lineage that is ready for persistence. This
 * class serves as an example to implement a planner.
 */
@ThreadSafe
public final class CheckpointLatestPlanner implements CheckpointPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(CheckpointLatestPlanner.class);

  /**
   * Creates a new instance of {@link CheckpointLatestPlanner}, which does not use the lineage store
   * view.
   *
   * @param lineageStoreView a view of a lineage store
   * @param fileSystemMasterView a view of the file system master
   */
  public CheckpointLatestPlanner(LineageStoreView lineageStoreView,
      FileSystemMasterView fileSystemMasterView) {}

  @Override
  public CheckpointPlan generatePlan(LineageStoreView store,
      FileSystemMasterView fileSystemMasterView) {
    Lineage toCheckpoint = null;
    long latestCreated = 0;
    for (Lineage lineage : store.getAllLineagesInTopologicalOrder()) {
      try {
        if (!LineageStateUtils.isCompleted(lineage, fileSystemMasterView)
            || LineageStateUtils.isPersisted(lineage, fileSystemMasterView)
            || LineageStateUtils.needRecompute(lineage, fileSystemMasterView)
            || LineageStateUtils.isInCheckpointing(lineage, fileSystemMasterView)) {
          continue;
        }
      } catch (FileDoesNotExistException | AccessControlException e) {
        LOG.error("The lineage file does not exist", e);
        continue;
      }
      if (lineage.getCreationTime() > latestCreated) {
        latestCreated = lineage.getCreationTime();
        toCheckpoint = lineage;
      }
    }

    return toCheckpoint == null ? new CheckpointPlan(new ArrayList<Long>())
        : new CheckpointPlan(Lists.newArrayList(toCheckpoint.getId()));
  }
}
