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
import alluxio.cli.fs.command.job.JobAttempt;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.JobConfig;
import alluxio.job.plan.migrate.MigrateConfig;
import alluxio.job.wire.JobInfo;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or directory specified by args.
 */
@ThreadSafe
@PublicApi
public class DistributedCpCommand extends AbstractDistributedJobCommand {
  private String mWriteType;

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedCpCommand(FileSystemContext fsContext) {
    super(fsContext);
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

    if (PathUtils.hasPrefix(dstPath.toString(), srcPath.toString())) {
      throw new RuntimeException(ExceptionMessage.MIGRATE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(
          srcPath, dstPath));
    }

    AlluxioConfiguration conf = mFsContext.getPathConf(dstPath);
    mWriteType = conf.get(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT);

    distributedCp(srcPath, dstPath);
    return 0;
  }

  private CopyJobAttempt newJob(String srcPath, String dstPath) {
    CopyJobAttempt jobAttempt = new CopyJobAttempt(mClient,
        new MigrateConfig(srcPath, dstPath, mWriteType, true),
        new CountingRetry(3));

    jobAttempt.run();

    return jobAttempt;
  }

  private void distributedCp(AlluxioURI srcPath, AlluxioURI dstPath)
      throws IOException, AlluxioException {
    if (mFileSystem.getStatus(srcPath).isFolder()) {
      createFolders(srcPath, dstPath);
    }
    copy(srcPath, dstPath);
    // Wait remaining jobs to complete.
    drain();
  }

  private void createFolders(AlluxioURI srcPath, AlluxioURI dstPath)
      throws IOException, AlluxioException {

    try {
      mFileSystem.createDirectory(dstPath);
      System.out.println("Created directory at " + dstPath.getPath());
    } catch (FileAlreadyExistsException e) {
      if (!mFileSystem.getStatus(dstPath).isFolder()) {
        throw e;
      }
    }

    for (URIStatus srcInnerStatus : mFileSystem.listStatus(srcPath)) {
      if (srcInnerStatus.isFolder()) {
        String dstInnerPath = computeTargetPath(srcInnerStatus.getPath(),
            srcPath.getPath(), dstPath.getPath());
        createFolders(new AlluxioURI(srcInnerStatus.getPath()), new AlluxioURI(dstInnerPath));
      }
    }
  }

  private void copy(AlluxioURI srcPath, AlluxioURI dstPath)
      throws IOException, AlluxioException {

    for (URIStatus srcInnerStatus : mFileSystem.listStatus(srcPath)) {
      String dstInnerPath = computeTargetPath(srcInnerStatus.getPath(),
          srcPath.getPath(), dstPath.getPath());
      if (srcInnerStatus.isFolder()) {
        copy(new AlluxioURI(srcInnerStatus.getPath()), new AlluxioURI(dstInnerPath));
      } else {
        addJob(srcInnerStatus.getPath(), dstInnerPath);
      }
    }
  }

  private void addJob(String srcPath, String dstPath) {
    if (mSubmittedJobAttempts.size() >= mActiveJobs) {
      // Wait one job to complete.
      waitJob();
    }
    mSubmittedJobAttempts.add(newJob(srcPath, dstPath));
    System.out.println("Copying " + srcPath + " to " + dstPath);
  }

  @Override
  public String getUsage() {
    return "distributedCp <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or directory in parallel at file level.";
  }

  private static String computeTargetPath(String path, String source, String destination)
      throws InvalidPathException {
    String relativePath = PathUtils.subtractPaths(path, source);

    return PathUtils.concatPath(destination, relativePath);
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
