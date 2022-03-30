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
import alluxio.grpc.CmdSummary;
import alluxio.grpc.OperationType;
import alluxio.job.CmdConfig;
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.job.cmd.persist.PersistCmdConfig;
import alluxio.job.wire.JobSource;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.common.CmdLogLink;
import alluxio.master.job.common.CmdProgress;
import alluxio.master.job.common.LogLink;
import alluxio.master.job.plan.PlanTracker;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CmdJobTracker to schedule a Cmd job to run.
 */
@ThreadSafe
public class CmdJobTracker {
  private static final Logger LOG = LoggerFactory.getLogger(CmdJobTracker.class);
  private final Map<Long, CmdProgress> mJobMap;
  private final Map<Long, LogLink> mLnk;
  private final Map<Long, CmdInfo> mInfoMap;
  private final DistLoadCliRunner mDistLoadCliRunner;
  private final MigrateCliRunner mMigrateCliRunner;
  private final PersistRunner mPersistRunner;
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
    mDistLoadCliRunner = new DistLoadCliRunner(mFsContext, jobMaster);
    mMigrateCliRunner = new MigrateCliRunner(mFsContext, jobMaster);
    mPersistRunner = new PersistRunner(mFsContext, jobMaster);
    mJobMap = Maps.newHashMap();
    mLnk = Maps.newHashMap();
    mInfoMap = Maps.newHashMap();
    mPlanTracker = planTracker;
  }

  /**
   * Constructor with runner providers.
   * @param fsContext Filesystem context
   * @param planTracker Plan tracker
   * @param distLoadCliRunner DistributedLoad runner
   * @param migrateCliRunner DistributedCopy runner
   * @param persistRunner Persist runner
   */
  public CmdJobTracker(FileSystemContext fsContext,
                       PlanTracker planTracker,
                       DistLoadCliRunner distLoadCliRunner,
                       MigrateCliRunner migrateCliRunner,
                       PersistRunner persistRunner) {
    mFsContext = fsContext;
    mDistLoadCliRunner = distLoadCliRunner;
    mMigrateCliRunner = migrateCliRunner;
    mPersistRunner = persistRunner;
    mJobMap = Maps.newHashMap();
    mLnk = Maps.newHashMap();
    mInfoMap = Maps.newHashMap();
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
      case DIST_LOAD:
        LoadCliConfig loadCliConfig = (LoadCliConfig) cmdConfig;
        int batchSize = loadCliConfig.getBatchSize();
        AlluxioURI filePath = new AlluxioURI(loadCliConfig.getFilePath());
        int replication = loadCliConfig.getReplication();
        Set<String> workerSet = loadCliConfig.getWorkerSet();
        Set<String> excludedWorkerSet = loadCliConfig.getExcludedWorkerSet();
        Set<String> localityIds =  loadCliConfig.getLocalityIds();
        Set<String> excludedLocalityIds = loadCliConfig.getExcludedLocalityIds();
        boolean directCache = loadCliConfig.getDirectCache();
        cmdInfo = mDistLoadCliRunner.runDistLoad(batchSize, filePath, replication, workerSet,
                excludedWorkerSet, localityIds, excludedLocalityIds, directCache, jobControlId);
        break;
      case DIST_CP:
        MigrateCliConfig migrateCliConfig = (MigrateCliConfig) cmdConfig;
        AlluxioURI srcPath = new AlluxioURI(migrateCliConfig.getSource());
        AlluxioURI dstPath = new AlluxioURI(migrateCliConfig.getDestination());
        LOG.info("run a dist cp command, cmd config is " + cmdConfig.toString());
        cmdInfo = mMigrateCliRunner.runDistCp(srcPath, dstPath,
            migrateCliConfig.getOverWrite(), migrateCliConfig.getBatchSize(),
            jobControlId);
        //createProgressEntry(cmdInfo);
        break;
      case PERSIST:
        PersistCmdConfig persistCmdConfig = (PersistCmdConfig) cmdConfig;
        cmdInfo = mPersistRunner.runPersistJob(persistCmdConfig, jobControlId);
        break;
      default:
        throw new JobDoesNotExistException(
                ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(cmdConfig.getName()));
    }

// test status and progress.
    mInfoMap.put(cmdInfo.getJobControlId(), cmdInfo);
  //  Status cmdStatus = getCmdStatus(cmdInfo.getJobControlId());
//    CmdProgress cmdProgress = getProgress(cmdInfo, true);
//    LOG.info(String.format("status is %s", cmdStatus.toString()));
//    cmdProgress.listAllProgress();
  }

  /**
   * Get CMD child job ids.
   * @param jobControlId
   * @return list of job ids
   */
  public List<Long> getChildJobIds(long jobControlId) throws JobDoesNotExistException {
    if (!mJobMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }
    CmdProgress progress = mJobMap.get(jobControlId);
    return progress.getChildJobIds();
  }

  /**
   * Get getCmdSummary.
   * @param jobControlId
   * @return getCmdSummary
   * @throws JobDoesNotExistException
   */
  public CmdSummary getCmdSummary(long jobControlId) throws JobDoesNotExistException {
    if (!mJobMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }
    CmdProgress cmdProgress = mJobMap.get(jobControlId);
    long currentTime = System.currentTimeMillis();
    return CmdSummary.newBuilder().setJobControlId(jobControlId)
            .setJobSource(cmdProgress.getJobSource().toProto())
            .setOperationType(cmdProgress.getOperationType())
            .setDuration(currentTime - cmdProgress.getSubmissionTime())
            .setCmdProgress(cmdProgress.toProto())
            .setErrorMessage(cmdProgress.getErrorMsg())
            .build();
  }

  /**
   * Get status information for a CMD.
   * @param jobControlId
   * @return the Command level status
   */
  public Status getCmdStatus(long jobControlId) throws JobDoesNotExistException {
    if (!mInfoMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    CmdInfo cmdInfo = mInfoMap.get(jobControlId);

    if (cmdInfo.getCmdRunAttempt().isEmpty()) { // If no attempts created, throws an Exception
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    int completed = 0;
    boolean finished = true;
    boolean failed = false;
    boolean canceled = false;
    Set<String> failedFiles = new HashSet<>(); // FAILED files
    for (CmdRunAttempt attempt : cmdInfo.getCmdRunAttempt()) {
      Status s = attempt.checkJobStatus();
      if (!s.isFinished()) {
        finished = false;
        break;
      }
      if (!failed && s == Status.FAILED) {
        failed = true;
        failedFiles.add(StringUtils.substringBetween(attempt.getJobConfig().toString(),
                "FilePath=", ","));
      }
      if (!canceled && s == Status.CANCELED) {
        canceled = true;
      }
      if (s == Status.COMPLETED) {
        completed++;
      }
    }

    if (finished) {
      if (failed) {
        return Status.FAILED; // FAILED has higher priority than CANCELED
      }
      if (canceled) {
        return Status.CANCELED;
      }
      if (completed == cmdInfo.getCmdRunAttempt().size()) {
        return Status.COMPLETED;
      }
    }

    if (failedFiles.isEmpty()) {
      LOG.warn("Failed file paths are:  ");
      failedFiles.forEach(LOG::warn);
    }

    return Status.RUNNING;
  }

  /**
   * @param statusList status list filter
   * @return cmd ids matching conditions
   */
  public Set<Long> findCmds(List<Status> statusList) throws JobDoesNotExistException {
    Set<Long> set = new HashSet<>();
    for (Map.Entry<Long, CmdInfo> x : mInfoMap.entrySet()) {
      if (statusList.isEmpty()
              || statusList.contains(getCmdStatus(x.getValue().getJobControlId()))) {
        Long key = x.getKey();
        set.add(key);
      }
    }
    return set;
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
    mJobMap.put(jobControlId, new CmdProgress(jobControlId,
            cmdInfo.getJobSource(), cmdInfo.getOperationType(), cmdInfo.getJobSubmissionTime()));
  }

  /**
   * Update progress for an entry.
   * @param jobControlId
   */
  public void updateProgress(long jobControlId) throws JobDoesNotExistException {
    if (!mInfoMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }
    CmdInfo info = mInfoMap.get(jobControlId);
    getProgress(info, false);
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

      // only create a progress entry when the job id is not null.
      if (jobId != null) {
        progress.createOrUpdateChildProgress(mPlanTracker, jobId, fileCount, fileSize,
                verbose);
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
