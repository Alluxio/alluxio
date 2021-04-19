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
import alluxio.grpc.ListStatusPOptions;
import alluxio.job.JobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
@PublicApi
public final class DistributedLoadCommand extends AbstractDistributedJobCommand {
  private static final int DEFAULT_REPLICATION = 1;
  private static final Option REPLICATION_OPTION =
      Option.builder()
          .longOpt("replication")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("replicas")
          .desc("Number of block replicas of each loaded file, default: " + DEFAULT_REPLICATION)
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
  private static final Option INDEX_FILE =
      Option.builder()
          .longOpt("index")
          .required(false)
          .hasArg(true)
          .numberOfArgs(0)
          .type(String.class)
          .argName("index file")
          .desc("Name of the index file that lists all files to be loaded")
          .build();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public DistributedLoadCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedLoad";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(REPLICATION_OPTION).addOption(ACTIVE_JOB_COUNT_OPTION)
            .addOption(INDEX_FILE);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "distributedLoad [--replication <num>] [--active-jobs <num>] [--index] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or all files in a directory into Alluxio space.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    mActiveJobs = FileSystemShellUtils.getIntArg(cl, ACTIVE_JOB_COUNT_OPTION,
            AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS);
    System.out.format("Allow up to %s active jobs%n", mActiveJobs);

    String[] args = cl.getArgs();
    int replication = FileSystemShellUtils.getIntArg(cl, REPLICATION_OPTION, DEFAULT_REPLICATION);

    if (!cl.hasOption("index")) {
      AlluxioURI path = new AlluxioURI(args[0]);
      distributedLoad(path, replication);
    } else {
      try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
        for (String filename; (filename = reader.readLine()) != null; ) {
          AlluxioURI path = new AlluxioURI(filename);
          distributedLoad(path, replication);
        }
      }
    }
    return 0;
  }

  @Override
  public void close() throws IOException {
    mClient.close();
  }

  /**
   * Creates a new job to load a file in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  private LoadJobAttempt newJob(AlluxioURI filePath, int replication) {
    LoadJobAttempt jobAttempt = new LoadJobAttempt(mClient, new
        LoadConfig(filePath.getPath(), replication), new CountingRetry(3));

    jobAttempt.run();

    return jobAttempt;
  }

  /**
   * Add one job.
   */
  private void addJob(URIStatus status, int replication) {
    AlluxioURI filePath = new AlluxioURI(status.getPath());
    if (status.getInAlluxioPercentage() == 100) {
      // The file has already been fully loaded into Alluxio.
      System.out.println(filePath + " is already fully loaded in Alluxio");
      return;
    }
    if (mSubmittedJobAttempts.size() >= mActiveJobs) {
      // Wait one job to complete.
      waitJob();
    }
    System.out.println(filePath + " loading");
    mSubmittedJobAttempts.add(newJob(filePath, replication));
  }

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  private void distributedLoad(AlluxioURI filePath, int replication)
      throws AlluxioException, IOException {
    load(filePath, replication);
    // Wait remaining jobs to complete.
    drain();
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException      when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int replication)
      throws IOException, AlluxioException {
    ListStatusPOptions options = ListStatusPOptions.newBuilder().setRecursive(true).build();
    mFileSystem.iterateStatus(filePath, options, uriStatus -> {
      if (!uriStatus.isFolder()) {
        addJob(uriStatus, replication);
      }
    });
  }

  private class LoadJobAttempt extends JobAttempt {
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
