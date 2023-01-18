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
import alluxio.job.CmdConfig;
import alluxio.job.cmd.load.LoadCliConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.job.cmd.persist.PersistCmdConfig;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.SimpleJobStatusBlock;
import alluxio.job.wire.Status;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CmdJobTracker to schedule a Cmd job to run.
 */
@ThreadSafe
public class CmdJobTracker {
  private static final Logger LOG = LoggerFactory.getLogger(CmdJobTracker.class);
  private final Map<Long, CmdInfo> mInfoMap = new ConcurrentHashMap<>(0, 0.95f,
      Math.max(8, 2 * Runtime.getRuntime().availableProcessors()));
  private final DistLoadCliRunner mDistLoadCliRunner;
  private final MigrateCliRunner mMigrateCliRunner;
  private final PersistRunner mPersistRunner;
  protected FileSystemContext mFsContext;
  public static final String DELIMITER = ",";

  /**
   * Create a new instance of {@link CmdJobTracker}.
   * @param fsContext filesystem context
   * @param jobMaster the job master
   */
  public CmdJobTracker(FileSystemContext fsContext,
                   JobMaster jobMaster) {
    mFsContext = fsContext;
    mDistLoadCliRunner = new DistLoadCliRunner(mFsContext, jobMaster);
    mMigrateCliRunner = new MigrateCliRunner(mFsContext, jobMaster);
    mPersistRunner = new PersistRunner(mFsContext, jobMaster);
  }

  /**
   * Constructor with runner providers.
   * @param fsContext Filesystem context
   * @param distLoadCliRunner DistributedLoad runner
   * @param migrateCliRunner DistributedCopy runner
   * @param persistRunner Persist runner
   */
  public CmdJobTracker(FileSystemContext fsContext,
                       DistLoadCliRunner distLoadCliRunner,
                       MigrateCliRunner migrateCliRunner,
                       PersistRunner persistRunner) {
    mFsContext = fsContext;
    mDistLoadCliRunner = distLoadCliRunner;
    mMigrateCliRunner = migrateCliRunner;
    mPersistRunner = persistRunner;
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
    CmdInfo cmdInfo;
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
        LOG.info("run a dist cp command, job control id: {}, cmd config: {}",
            jobControlId, cmdConfig);
        cmdInfo = mMigrateCliRunner.runDistCp(srcPath, dstPath,
            migrateCliConfig.getOverWrite(), migrateCliConfig.getBatchSize(),
            jobControlId);
        break;
      case PERSIST:
        PersistCmdConfig persistCmdConfig = (PersistCmdConfig) cmdConfig;
        cmdInfo = mPersistRunner.runPersistJob(persistCmdConfig, jobControlId);
        break;
      default:
        throw new JobDoesNotExistException(
                ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(cmdConfig.getName()));
    }

    mInfoMap.put(cmdInfo.getJobControlId(), cmdInfo);
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

    if (cmdInfo.getCmdRunAttempt().isEmpty()) { // If no attempts created,
      // that means the files are loaded already, set status to complete
      return Status.COMPLETED;
    }

    int completed = 0;
    boolean finished = true;
    boolean failed = false;
    boolean canceled = false;
    for (CmdRunAttempt attempt : cmdInfo.getCmdRunAttempt()) {
      Status s = attempt.checkJobStatus();
      if (!s.isFinished()) {
        finished = false;
        break;
      }
      if (!failed && s == Status.FAILED) {
        failed = true;
        attempt.printFailed();
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

    return Status.RUNNING;
  }

  /**
   * @param statusList status list filter
   * @return cmd ids matching conditions
   */
  public Set<Long> findCmdIds(List<Status> statusList) throws JobDoesNotExistException {
    Set<Long> set = new HashSet<>();
    for (Map.Entry<Long, CmdInfo> x : mInfoMap.entrySet()) {
      if (statusList.isEmpty()
              || statusList.contains(getCmdStatus(
                      x.getValue().getJobControlId()))) {
        Long key = x.getKey();
        set.add(key);
      }
    }
    return set;
  }

  /**
   * @return all failed file paths
   */
  public Set<String> findAllFailedPaths() {
    Set<String> set = new HashSet<>();
    for (Map.Entry<Long, CmdInfo> x : mInfoMap.entrySet()) {
      long jobControlId = x.getKey();
      try {
        set.addAll(findFailedPaths(jobControlId));
      } catch (JobDoesNotExistException e) {
        LOG.info("skip because of no such a command id.");
      }
    }
    return set;
  }

  /**
   * @param jobControlId jobControlId
   * @return failed file paths
   */
  public Set<String> findFailedPaths(long jobControlId) throws JobDoesNotExistException {
    if (!mInfoMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    CmdInfo cmdInfo = mInfoMap.get(jobControlId);

    if (cmdInfo.getCmdRunAttempt().isEmpty()) { // If no attempts are created, throws an Exception
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    return cmdInfo.getCmdRunAttempt().stream()
            .map(CmdRunAttempt::getFailedFiles).flatMap(Collection::stream)
            .collect(Collectors.toSet());
  }

  /**
   * Get a cmdStatusBlock information.
   * @param jobControlId command id
   * @return CmdStatusBlock
   */
  public CmdStatusBlock getCmdStatusBlock(long jobControlId)
          throws JobDoesNotExistException {
    if (!mInfoMap.containsKey(jobControlId)) {
      throw new JobDoesNotExistException(
              ExceptionMessage.JOB_DEFINITION_DOES_NOT_EXIST.getMessage(jobControlId));
    }

    CmdInfo cmdInfo = mInfoMap.get(jobControlId);

    if (cmdInfo.getCmdRunAttempt().isEmpty()) { // If no attempts are created,
      // that means the files are loaded already, set status to complete
      return new CmdStatusBlock(cmdInfo.getJobControlId(), Collections.EMPTY_LIST,
              cmdInfo.getOperationType());
    }

    List<SimpleJobStatusBlock> blockList = cmdInfo.getCmdRunAttempt().stream()
            .map(attempt -> new SimpleJobStatusBlock(attempt.getJobId(),
                    attempt.checkJobStatus(),
                    attempt.getFilePath(),
                    String.join(DELIMITER, attempt.getFailedFiles())))
            .collect(Collectors.toList());
    return new CmdStatusBlock(cmdInfo.getJobControlId(), blockList, cmdInfo.getOperationType());
  }
}
