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

package alluxio.master.job.tracker;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.JobDoesNotExistException;
import alluxio.grpc.OperationType;
import alluxio.job.CmdConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.job.wire.JobSource;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.common.CmdLogLink;
import alluxio.master.job.common.CmdProgress;
import alluxio.master.job.common.LogLink;
import alluxio.master.job.plan.PlanTracker;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CmdJobTracker to schedule a Cmd job to run.
 */
@ThreadSafe
public class CmdJobTracker {
  private static final Logger LOG = LoggerFactory.getLogger(CmdJobTracker.class);
  private final Map<Long, CmdProgress> mJobMap;
  private final Map<Long, LogLink> mLnk;
  private final MigrateCliRunner mMigrateCliRunner;
  private PlanTracker mPlanTracker;
  protected FileSystemContext mFsContext;

  /**
   * Create a new instance of {@link CmdJobTracker}.
   * @param fsContext filesystem context
   * @param jobMaster the job master
   * @param planTracker plan tracker
   */
  public CmdJobTracker(FileSystemContext fsContext,
                   JobMaster jobMaster, PlanTracker planTracker) {

    mFsContext = fsContext;
    mMigrateCliRunner = new MigrateCliRunner(mFsContext, jobMaster);
    mJobMap = Maps.newHashMap();
    mLnk = Maps.newHashMap();
    mPlanTracker = planTracker;
  }

  /**
   * Run the PlanTracker to trigger a Cmd job based on CmdConfig.
   * @param cmdConfig the distributed command job config
   * @param jobControlId job control id for the command
   */
  public synchronized void run(CmdConfig cmdConfig, long jobControlId) throws
          JobDoesNotExistException, IOException {
    runDistributedCommand(cmdConfig, jobControlId);
  }

  private void runDistributedCommand(CmdConfig cmdConfig, long jobControlId)
          throws JobDoesNotExistException, IOException {
    CmdInfo cmdInfo = null;
    switch (cmdConfig.getOperationType()) {
//      case DIST_LOAD: // todo
//        break;
      case DIST_CP:
        MigrateCliConfig migrateCliConfig = (MigrateCliConfig) cmdConfig;
        AlluxioURI srcPath = new AlluxioURI(migrateCliConfig.getSource());
        AlluxioURI dstPath = new AlluxioURI(migrateCliConfig.getDestination());
        LOG.info("run a dist cp command, cmd config is " + cmdConfig.toString());
        cmdInfo = mMigrateCliRunner.runDistCp(srcPath, dstPath,
            migrateCliConfig.getOverWrite(), migrateCliConfig.getBatchSize(),
            jobControlId);
        createProgressEntry(cmdInfo);
        break;
//      case PERSIST: // todo
//        break;
      default:
        throw new JobDoesNotExistException(
                ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(cmdConfig.getName()));
    }

// test status and progress.
//    Status cmdStatus = getCmdStatus(cmdInfo);
//    CmdProgress cmdProgress = getProgress(cmdInfo, true);
//    LOG.info(String.format("status is %s", cmdStatus.toString()));
//    cmdProgress.listAllProgress();
  }

  /**
   * Get status information for a CMD.
   * @param cmdInfo
   * @return the Command level status
   */
  public Status getCmdStatus(CmdInfo cmdInfo) throws JobDoesNotExistException {
    long jobControlId = cmdInfo.getJobControlId();
    if (!mJobMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    CmdProgress progress = mJobMap.get(jobControlId);
    return progress.consolidateStatus();
  }

  /**
   * Get submission time.
   * @param cmdInfo
   * @return submission time
   */
  public long getSubmissionTime(CmdInfo cmdInfo) {
    return cmdInfo.getJobSubmissionTime();
  }

  /**
   * Get job source.
   * @param cmdInfo
   * @return job source
   */
  public JobSource getJobSource(CmdInfo cmdInfo) {
    return cmdInfo.getJobSource();
  }

  /**
   * Get operation type.
   * @param cmdInfo
   * @return operation type
   */
  public OperationType getOperationType(CmdInfo cmdInfo) {
    return cmdInfo.getOperationType();
  }

  /**
   * Get file path.
   * @param cmdInfo
   * @return file path
   */
  public List<String> getFilePath(CmdInfo cmdInfo) {
    return cmdInfo.getFilePath();
  }

  /**
   * Create a progress entry in the map.
   * @param cmdInfo
   */
  public void createProgressEntry(CmdInfo cmdInfo) {
    long jobControlId = cmdInfo.getJobControlId();
    mJobMap.put(jobControlId, new CmdProgress(jobControlId));
  }

  /**
   * Get progress of a distributed command job. Need to update progress.
   * @param cmdInfo
   * @param verbose
   * @return the progress of a distributed command
   */
  public CmdProgress getProgress(CmdInfo cmdInfo, boolean verbose) throws JobDoesNotExistException {
    long jobControlId = cmdInfo.getJobControlId();
    if (!mJobMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    CmdProgress progress = mJobMap.get(jobControlId);

    for (CmdRunAttempt attempt : cmdInfo.getCmdRunAttempt()) {
      Long jobId = attempt.getJobId();
      long fileCount = attempt.getFileCount();
      long fileSize = attempt.getFileSize();
      LOG.info("file count " + fileCount);
      LOG.info("file size " + fileSize);
      if (jobId != null) {
        progress.createOrUpdateChildProgress(mPlanTracker, jobId, fileCount, fileSize, verbose);
      }
    }
    return progress;
  }

  /**
   * Get a map of loglink.
   * @param cmdInfo
   * @return log link map
   */
  public LogLink getLogLink(CmdInfo cmdInfo) throws JobDoesNotExistException {
    long jobControlId = cmdInfo.getJobControlId();
    if (!mLnk.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }
    return mLnk.get(jobControlId);
  }

  /**
   * Create a loglink entry to the mLnk map.
   * @param cmdInfo
   */
  public void createLogLinkEntry(CmdInfo cmdInfo) {
    long jobControlId = cmdInfo.getJobControlId();
    mLnk.put(jobControlId, new CmdLogLink(jobControlId));
    LogLink cmdLogLink = mLnk.get(jobControlId);
    cmdInfo.getCmdRunAttempt().forEach(cmdRunAttempt -> {
      long id = cmdRunAttempt.getJobId();
      cmdLogLink.addLinkForChildJob(id, ""); // todo: add url for the child job.
    });
  }
}
