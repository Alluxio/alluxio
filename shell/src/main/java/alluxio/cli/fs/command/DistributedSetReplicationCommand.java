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

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.JobConfig;
import alluxio.job.plan.replicate.ReplicateConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * Set replication for a file or directory in Alluxio space.
 */
@ThreadSafe
@PublicApi
public class DistributedSetReplicationCommand extends AbstractDistributedJobCommand {
  private static final int DEFAULT_REPLICATION = -1;
  private static final Option REPLICATION_OPTION =
      Option.builder()
          .longOpt("replication")
          .required(true)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("replicas")
          .desc("Number of block replicas of each loaded file")
          .build();
  private static final Option ACTIVE_JOB_COUNT_OPTION =
      Option.builder()
          .longOpt("active-jobs")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("active job count")
          .desc("Number of active jobs that can run at the same time. Later jobs must wait. "
              + "The default upper limit is "
              + AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS)
          .build();

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedSetReplicationCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ACTIVE_JOB_COUNT_OPTION)
        .addOption(REPLICATION_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    mActiveJobs = FileSystemShellUtils.getIntArg(cl, ACTIVE_JOB_COUNT_OPTION,
        AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS);
    System.out.format("Allow up to %s active jobs%n", mActiveJobs);
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    int replication = FileSystemShellUtils.getIntArg(cl, REPLICATION_OPTION, DEFAULT_REPLICATION);
    distributedSetRep(path, replication);
    return 0;
  }

  @Override
  public String getCommandName() {
    return "distributedSetRep";
  }

  @Override
  public String getUsage() {
    return getCommandName() + "[--active-jobs <num>] <--replication <replicas>> <path>";
  }

  @Override
  public String getDescription() {
    return "for a file or directory in Alluxio space";
  }

  private void distributedSetRep(AlluxioURI path, int replicas)
      throws IOException, AlluxioException {
    setRep(path, replicas);
    // Wait remaining jobs to complete.
    drain();
  }

  private void setRep(AlluxioURI path, int replicas)
      throws IOException, AlluxioException {

    for (URIStatus innerStatus : mFileSystem.listStatus(path)) {
      if (innerStatus.isFolder()) {
        setRep(new AlluxioURI(innerStatus.getPath()), replicas);
      } else {
        for (long blockId : innerStatus.getBlockIds()) {
          addJob(innerStatus.getPath(), blockId, replicas);
        }
        if (innerStatus.getBlockIds().size() > 0) {
          System.out.println("Replicate " + innerStatus.getPath() + " to " + replicas);
        }
      }
    }
  }

  private void addJob(String path, long blockId, int replicas) {
    if (mSubmittedJobAttempts.size() >= mActiveJobs) {
      // Wait one job to complete.
      waitJob();
    }
    mSubmittedJobAttempts.add(newJob(path, blockId, replicas));
  }

  private ReplicateJobAttempt newJob(String path, long blockId, int replicas) {
    ReplicateJobAttempt
        jobAttempt = new ReplicateJobAttempt(mClient,
        new ReplicateConfig(path, blockId, replicas),
        new CountingRetry(3));

    jobAttempt.run();

    return jobAttempt;
  }

  private class ReplicateJobAttempt extends JobAttempt {
    private ReplicateConfig mJobConfig;

    ReplicateJobAttempt(
        JobMasterClient client, ReplicateConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    public void logFailedAttempt(JobInfo jobInfo) {
      System.out.println(
          String.format("Attempt %d to set replication %s to %s failed because: %s",
          mRetryPolicy.getAttemptCount(), mJobConfig.getPath(), mJobConfig.getReplicas(),
          jobInfo.getErrorMessage()));
    }

    @Override
    protected void logFailed() {
      System.out.println(
          String.format("Failed to complete set replication %s to %s after %d retries.",
          mJobConfig.getPath(), mJobConfig.getReplicas(), mRetryPolicy.getAttemptCount()));
    }

    @Override
    public void logCompleted() {
      System.out.println(String.format("Successfully set replication %s to %s after %d attempts",
          mJobConfig.getPath(), mJobConfig.getReplicas(), mRetryPolicy.getAttemptCount()));
    }
  }
}
