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
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.grpc.ListStatusPOptions;
import alluxio.job.JobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import java.io.IOException;

/**
 * Utilities Loads a file or directory in Alluxio space,
 * makes it resident in memory.
 */
public final class DistributedLoadUtils {

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param command The command to execute loading
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  public static void distributedLoad(AbstractDistributedJobCommand command,
      AlluxioURI filePath, int replication) throws AlluxioException, IOException {
    load(command, filePath, replication);
    // Wait remaining jobs to complete.
    command.drain();
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param command The command to execute loading
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException      when non-Alluxio exception occurs
   */
  private static void load(AbstractDistributedJobCommand command,
      AlluxioURI filePath, int replication) throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    command.mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder()) {
        addJob(command, uriStatus, replication);
      }
    });
  }

  private static void addJob(AbstractDistributedJobCommand command,
      URIStatus status, int replication) {
    AlluxioURI filePath = new AlluxioURI(status.getPath());
    if (status.getInAlluxioPercentage() == 100) {
      // The file has already been fully loaded into Alluxio.
      System.out.println(filePath + " is already fully loaded in Alluxio");
      return;
    }
    if (command.mSubmittedJobAttempts.size() >= command.mActiveJobs) {
      // Wait one job to complete.
      command.waitJob();
    }
    System.out.println(filePath + " loading");
    command.mSubmittedJobAttempts.add(newJob(command, filePath, replication));
  }

  /**
   * Creates a new job to load a file in Alluxio space, makes it resident in memory.
   *
   * @param command The command to execute loading
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  private static LoadJobAttempt newJob(AbstractDistributedJobCommand command,
      AlluxioURI filePath, int replication) {
    LoadJobAttempt jobAttempt = new LoadJobAttempt(command.mClient, new
        LoadConfig(filePath.getPath(), replication), new CountingRetry(3));

    jobAttempt.run();

    return jobAttempt;
  }

  private static class LoadJobAttempt extends JobAttempt {
    private LoadConfig mJobConfig;

    LoadJobAttempt(JobMasterClient client, LoadConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    protected void logFailedAttempt(JobInfo jobInfo) {
      System.out.println(String.format("Attempt %d to load %s failed because: %s",
          mRetryPolicy.getAttemptCount(), mJobConfig.getFilePath(),
          jobInfo.getErrorMessage()));
    }

    @Override
    protected void logFailed() {
      System.out.println(String.format("Failed to complete loading %s after %d retries.",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount()));
    }

    @Override
    protected void logCompleted() {
      System.out.println(String.format("Successfully loaded path %s after %d attempts",
          mJobConfig.getFilePath(), mRetryPolicy.getAttemptCount()));
    }
  }
}
