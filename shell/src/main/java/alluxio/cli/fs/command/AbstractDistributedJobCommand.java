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

package alluxio.cli.fs.command;

import alluxio.ClientContext;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobMasterClient;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The base class for all the distributed job based {@link alluxio.cli.Command} classes.
 * It provides handling for submitting multiple jobs and handling retries of them.
 */
public abstract class AbstractDistributedJobCommand extends AbstractFileSystemCommand {
  protected static final int DEFAULT_ACTIVE_JOBS = 3000;

  protected List<JobAttempt> mSubmittedJobAttempts;
  protected int mActiveJobs;
  protected final JobMasterClient mClient;
  private int mFailedCount;
  private int mCompletedCount;

  protected AbstractDistributedJobCommand(FileSystemContext fsContext) {
    super(fsContext);
    mSubmittedJobAttempts = Lists.newArrayList();
    final ClientContext clientContext = mFsContext.getClientContext();
    mClient = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(clientContext).build());
    mActiveJobs = DEFAULT_ACTIVE_JOBS;
    mFailedCount = 0;
    mCompletedCount = 0;
  }

  protected void drain() {
    while (!mSubmittedJobAttempts.isEmpty()) {
      waitJob();
    }
  }

  /**
   * Waits for at least one job to complete.
   */
  protected void waitJob() {
    AtomicBoolean removed = new AtomicBoolean(false);
    while (true) {
      mSubmittedJobAttempts = mSubmittedJobAttempts.stream().filter((jobAttempt) -> {
        Status check = jobAttempt.check();
        switch (check) {
          case CREATED:
          case RUNNING:
            return true;
          case CANCELED:
          case COMPLETED:
            mCompletedCount++;
            removed.set(true);
            return false;
          case FAILED:
            mFailedCount++;
            removed.set(true);
            return false;
          default:
            throw new IllegalStateException(String.format("Unexpected Status: %s", check));
        }
      }).collect(Collectors.toList());
      if (removed.get()) {
        return;
      }
      CommonUtils.sleepMs(5);
    }
  }

  /**
   * Gets the number of failed jobs.
   * @return number of failed jobs
   */
  public int getFailedCount() {
    return mFailedCount;
  }

  /**
   * Gets the number of completed jobs.
   * @return the number of completed job
   */
  public int getCompletedCount() {
    return mCompletedCount;
  }
}
