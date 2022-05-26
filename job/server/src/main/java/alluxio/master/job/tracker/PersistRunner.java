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

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.OperationType;
import alluxio.job.cmd.persist.PersistCmdConfig;
import alluxio.job.wire.JobSource;
import alluxio.master.job.JobMaster;
import alluxio.master.job.common.CmdInfo;
import alluxio.master.job.metrics.DistributedCmdMetrics;
import alluxio.retry.CountingRetry;

import com.google.common.collect.Lists;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A config runner for a Persist job.
 */
public class PersistRunner {
  static final long DEFAULT_FILE_COUNT = 1;

  private final FileSystem mFileSystem;
  private final JobMaster mJobMaster;

  /**
   * constructor.
   * @param fsContext
   * @param jobMaster
   */
  public PersistRunner(@Nullable FileSystemContext fsContext, JobMaster jobMaster) {
    if (fsContext == null) {
      fsContext = FileSystemContext.create();
    }
    mFileSystem = FileSystem.Factory.create(fsContext);
    mJobMaster = jobMaster;
  }

  /**
   * Run a Persist job.
   * @param config the config of persist operation
   * @param jobControlId THe parent level job control ID
   * @return CmdInfo
   */
  public CmdInfo runPersistJob(PersistCmdConfig config,
                            long jobControlId) throws IOException {
    long submissionTime = System.currentTimeMillis();
    CmdInfo cmdInfo = new CmdInfo(jobControlId, OperationType.PERSIST, JobSource.SYSTEM,
            submissionTime, Lists.newArrayList(config.getFilePath()));

    CmdRunAttempt attempt = new CmdRunAttempt(new CountingRetry(3), mJobMaster);
    setJobConfigAndFileMetrics(config, attempt);

    cmdInfo.addCmdRunAttempt(attempt);
    attempt.run();
    return cmdInfo;
  }

  // Create a JobConfig and set file count and size for the AsyncPersist job.
  private void setJobConfigAndFileMetrics(PersistCmdConfig config, CmdRunAttempt attempt) {
    long fileCount = DEFAULT_FILE_COUNT; // file count is default 1
    long fileSize = DistributedCmdMetrics.getFileSize(config.getFilePath(),
            mFileSystem, new CountingRetry(3));
    attempt.setFileCount(fileCount);
    attempt.setFileSize(fileSize);
    attempt.setConfig(config.toPersistConfig());
  }
}
