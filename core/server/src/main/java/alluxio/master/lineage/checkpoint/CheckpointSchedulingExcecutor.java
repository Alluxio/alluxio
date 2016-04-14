/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.lineage.checkpoint;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.MasterContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.lineage.LineageMaster;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Schedules a checkpoint plan.
 */
@NotThreadSafe
public final class CheckpointSchedulingExcecutor implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Configuration mConfiguration;
  private final LineageMaster mLineageMaster;
  private final FileSystemMaster mFileSystemMaster;
  private final CheckpointPlanner mPlanner;

  /**
   * @param lineageMaster the master for lineage
   * @param fileSystemMaster the master for the file system
   */
  public CheckpointSchedulingExcecutor(LineageMaster lineageMaster,
      FileSystemMaster fileSystemMaster) {
    mLineageMaster = Preconditions.checkNotNull(lineageMaster);
    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
    mConfiguration = MasterContext.getConf();
    mPlanner =
        CheckpointPlanner.Factory.create(mConfiguration, mLineageMaster.getLineageStoreView(),
            mFileSystemMaster.getFileSystemMasterView());
  }

  @Override
  public void heartbeat() {
    CheckpointPlan plan = mPlanner.generatePlan(mLineageMaster.getLineageStoreView(),
        mFileSystemMaster.getFileSystemMasterView());
    if (!plan.isEmpty()) {
      LOG.info("Checkpoint scheduler created the plan: {}", plan);
    }
    mLineageMaster.scheduleCheckpoint(plan);
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
