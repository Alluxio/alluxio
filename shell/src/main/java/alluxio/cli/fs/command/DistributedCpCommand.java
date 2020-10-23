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
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.job.JobGrpcClientUtils;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.JobConfig;
import alluxio.job.plan.load.LoadConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.RetryPolicy;
import alluxio.util.CommonUtils;

import alluxio.worker.job.JobMasterClientContext;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or directory specified by args.
 */
@ThreadSafe
@PublicApi
public final class DistributedCpCommand extends AbstractFileSystemCommand {
  private static final int DEFAULT_ACTIVE_JOBS = 1000;

  private List<JobAttempt> mSubmittedJobAttempts;
  private int mActiveJobs;
  private JobMasterClient mClient;

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedCpCommand(FileSystemContext fsContext) {
    super(fsContext);
    mSubmittedJobAttempts = Lists.newArrayList();
    final ClientContext clientContext = mFsContext.getClientContext();
    mClient = JobMasterClient.Factory.create(
        JobMasterClientContext.newBuilder(clientContext).build());
  }

  @Override
  public String getCommandName() {
    return "distributedCp";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);
    Thread thread = CommonUtils.createProgressThread(2L * Constants.SECOND_MS, System.out);
    thread.start();
    try {
      AlluxioConfiguration conf = mFsContext.getPathConf(dstPath);
      JobGrpcClientUtils.run(new MigrateConfig(srcPath.getPath(), dstPath.getPath(),
          conf.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT), true,
          false), 1, mFsContext.getPathConf(dstPath));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    } finally {
      thread.interrupt();
    }
    System.out.println("Copied " + srcPath + " to " + dstPath);
    return 0;
  }

  @Override
  public String getUsage() {
    return "distributedCp <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or directory in parallel at file level.";
  }

  private class CopyJobAttempt extends JobAttempt {
    private MigrateConfig mJobConfig;


    CopyJobAttempt(JobMasterClient client, MigrateConfig jobConfig, RetryPolicy retryPolicy) {
      super(client, retryPolicy);
      mJobConfig = jobConfig;
    }

    @Override
    protected JobConfig getJobConfig() {
      return mJobConfig;
    }

    @Override
    public void logFailedAttempt(JobInfo jobInfo) {
      System.out.println(String.format("Attempt %d to copy %s to %s failed because: %s",
          mRetryPolicy.getAttemptCount(), mJobConfig.getSource(), mJobConfig.getDestination(),
          jobInfo.getErrorMessage()));
    }

    @Override
    protected void logFailed() {
      System.out.println(String.format("Failed to complete copying %s to %s after %d retries.",
          mJobConfig.getSource(), mJobConfig.getDestination(), mRetryPolicy.getAttemptCount()));
    }

    @Override
    public void logCompleted() {
      System.out.println(String.format("Successfully copied %s to %s after %d attempts",
          mJobConfig.getSource(), mJobConfig.getDestination(), mRetryPolicy.getAttemptCount()));
    }
  }
}
