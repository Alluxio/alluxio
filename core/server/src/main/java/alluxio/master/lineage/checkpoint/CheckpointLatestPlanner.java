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

package alluxio.master.lineage.checkpoint;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import alluxio.Constants;
import alluxio.exception.FileDoesNotExistException;
import alluxio.master.file.meta.FileSystemMasterView;
import alluxio.master.lineage.meta.Lineage;
import alluxio.master.lineage.meta.LineageStateUtils;
import alluxio.master.lineage.meta.LineageStoreView;

/**
 * This class tries to checkpoint the latest created lineage that is ready for persistence. This
 * class serves as an example to implement a planner.
 */
@ThreadSafe
public final class CheckpointLatestPlanner implements CheckpointPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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
      } catch (FileDoesNotExistException e) {
        LOG.error("The lineage file does not exist", e);
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
