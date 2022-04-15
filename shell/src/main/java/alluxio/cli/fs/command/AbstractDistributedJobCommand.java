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
import alluxio.job.CmdConfig;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.SimpleJobStatusBlock;
import alluxio.job.wire.Status;
import alluxio.util.CommonUtils;
import alluxio.worker.job.JobMasterClientContext;

import com.google.common.collect.Lists;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The base class for all the distributed job based {@link alluxio.cli.Command} classes.
 * It provides handling for submitting multiple jobs and handling retries of them.
 */
public abstract class AbstractDistributedJobCommand extends AbstractFileSystemCommand {
  protected static final int DEFAULT_ACTIVE_JOBS = 3000;
  private static final int DEFAULT_FAILURE_LIMIT = 20;
  protected static final Option NOT_WAIT_OPTION =
          Option.builder()
                  .longOpt("not-wait")
                  .required(false)
                  .hasArg(false)
                  .argName("not-wait")
                  .desc("Use not-wait to submit the command"
                          + " asynchronously and not wait for command to finish")
                  .build();

  protected List<JobAttempt> mSubmittedJobAttempts;
  protected int mActiveJobs;
  protected final JobMasterClient mClient;
  private int mFailedCount;
  private int mCompletedCount;
  private int mFailedCmdCount; // user only knows command failure count from the client side
  private int mCompletedCmdCount; // user only knows command success count from the client side
  private Set<String> mFailedFiles;

  protected AbstractDistributedJobCommand(FileSystemContext fsContext) {
    super(fsContext);
    mSubmittedJobAttempts = Lists.newArrayList();
    final ClientContext clientContext = mFsContext.getClientContext();
    mClient =
        JobMasterClient.Factory.create(JobMasterClientContext.newBuilder(clientContext).build());
    mActiveJobs = DEFAULT_ACTIVE_JOBS;
    mFailedCount = 0;
    mCompletedCount = 0;
    mFailedCmdCount = 0;
    mCompletedCmdCount = 0;
    mFailedFiles = new HashSet<>();
  }

  protected void drain() {
    while (!mSubmittedJobAttempts.isEmpty()) {
      waitJob();
    }
  }

  protected Long submit(CmdConfig cmdConfig) {
    Long jobControlId = null;
    try {
      jobControlId = mClient.submit(cmdConfig);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return jobControlId;
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
            mCompletedCount += jobAttempt.getSize();
            removed.set(true);
            return false;
          case FAILED:
            Set<String> failedFiles = jobAttempt.getFailedFiles();
            mCompletedCount += (jobAttempt.getSize() - failedFiles.size());
            mFailedCount += failedFiles.size();
            mFailedFiles.addAll(failedFiles);
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
   * Waits for command to complete.
   */
  protected void waitForCmd(long jobControlId) {
    boolean error = false;
    while (true) {
      try {
        Status check = mClient.getCmdStatus(jobControlId);
        if (check.equals(Status.FAILED)) {
          mFailedCmdCount++;
          break;
        }
        if (check.equals(Status.COMPLETED)) {
          mCompletedCmdCount++;
          break;
        }
        if (check.equals(Status.CANCELED)) {
          break;
        }
      } catch (IOException e) {
        error = true;
        System.out.println(String.format("Unable to get command status for %s,"
                + "the files may already be loaded in Alluxio,"
                + " please retry using `getCmdStatus` to check command detailed status,"
                + " or using `fs ls` command to check if the files are already loaded."
                + "%n", jobControlId));
        break;
      }
      CommonUtils.sleepMs(5);
    }

    if (!error) {
      System.out.format("Finished running the command, jobControlId = %s%n",
              jobControlId);
      try {
        getDetailedCmdStatus(jobControlId);
      } catch (IOException e) {
//        System.out.println(String.format("Unable to get detailed command status for %s,"
//                + "the files may already be loaded in Alluxio,"
//                + " please retry using `getCmdStatus` to check command detailed status,"
//                + " or using `fs ls` command to check if the files are already loaded."
//                + "%n", jobControlId));
      }
    }
  }

  /**
   * Get detailed information about a command.
   * @param jobControlId
   */
  protected void getDetailedCmdStatus(long jobControlId) throws IOException {
    CmdStatusBlock cmdStatus = mClient.getCmdStatusDetailed(jobControlId);
    List<SimpleJobStatusBlock> blockList = cmdStatus.getJobStatusBlock();

    List<SimpleJobStatusBlock> failedBlocks = blockList.stream()
            .filter(block -> {
              String failed = block.getFilesPathFailed();
              if (failed.equals("") || failed == null) {
                return false;
              }
              return true;
            }).collect(Collectors.toList());

    failedBlocks.forEach(block -> {
      String[] files = block.getFilesPathFailed().split(",");
      mFailedFiles.addAll(Arrays.asList(files));
    });

    mFailedCount = mFailedFiles.size();

    List<SimpleJobStatusBlock> nonEmptyPathBlocks = blockList.stream()
            .filter(block -> {
              String path = block.getFilePath();
              if (path.equals("") || path == null) {
                return false;
              }
              return true;
            }).collect(Collectors.toList());

    List<String> completedFiles = Lists.newArrayList();

    nonEmptyPathBlocks.stream().forEach(block -> {
      String[] paths = block.getFilePath().split(",");
      for (String path: paths) {
        if (!mFailedFiles.contains(path)) {
          completedFiles.add(path);
        }
      }
    });

    mCompletedCount = completedFiles.size();

    if (cmdStatus.getJobStatusBlock().isEmpty()) {
      System.out.format("Unable to get command status for jobControlId=%s, please retry"
                + " or use `fs ls` command to check if files are already loaded in Alluxio.%n",
              jobControlId);
    } else {
      System.out.format("Get command status information below: %n");

      completedFiles.forEach(file -> {
        System.out.format("Successfully loaded path %s%n", file);
      });

      System.out.format("Total completed file count is %s, failed file count is %s%n",
              mCompletedCount, mFailedCount);
    }
  }

  protected void processFailures(String arg, Set<String> failures, String logFileLocation) {
    String path = String.join("_", StringUtils.split(arg, "/"));
    String failurePath = String.format(logFileLocation, path);
    StringBuilder output = new StringBuilder();
    output.append("Here are failed files: \n");
    Iterator<String> iterator = failures.iterator();
    for (int i = 0; i < Math.min(DEFAULT_FAILURE_LIMIT, failures.size()); i++) {
      String failure = iterator.next();
      output.append(failure);
      output.append(",\n");
    }
    output.append(String.format("Check out %s for full list of failed files.", failurePath));
    System.out.print(output);
    try (FileOutputStream writer = FileUtils.openOutputStream(new File(failurePath))) {
      for (String failure : failures) {
        writer.write(String.format("%s%n", failure).getBytes(StandardCharsets.UTF_8));
      }
    } catch (Exception e) {
      System.out.println("Exception writing failure files:");
      System.out.println(e.getMessage());
    }
  }

  /**
   * Gets the number of completed jobs.
   * @return the number of completed job
   */
  public int getCompletedCount() {
    return mCompletedCount;
  }

  /**
   * Gets the number of failed jobs.
   * @return number of failed jobs
   */
  public int getFailedCount() {
    return mFailedCount;
  }

  /**
   * Gets the number of failed commands.
   * @return number of failed commands
   */
  public int getFailedCmdCount() {
    return mFailedCmdCount;
  }

  /**
   * Gets the number of completed commands.
   * @return the number of completed commands
   */
  public int getCompletedCmdCount() {
    return mCompletedCmdCount;
  }

  /**
   * Gets failed files.
   * @return failed files
   */
  public Set<String> getFailedFiles() {
    return mFailedFiles;
  }
}
