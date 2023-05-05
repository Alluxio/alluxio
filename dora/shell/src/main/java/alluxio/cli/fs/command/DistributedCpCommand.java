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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.CmdConfig;
import alluxio.job.cmd.migrate.MigrateCliConfig;
import alluxio.util.io.PathUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or directory specified by args.
 */
@ThreadSafe
@PublicApi
public class DistributedCpCommand extends AbstractDistributedJobCommand {
  private static final String DEFAULT_FAILURE_FILE_PATH =
          "./logs/user/distributedCp_%s_failures.csv";
  private WriteType mWriteType;

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

  private static final Option OVERWRITE_OPTION =
      Option.builder()
          .longOpt("overwrite")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Boolean.class)
          .argName("overwrite")
          .desc("Whether to overwrite the destination. Default is true.")
          .build();

  private static final Option BATCH_SIZE_OPTION =
      Option.builder()
          .longOpt("batch-size")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .type(Number.class)
          .argName("batch-size")
          .desc("Number of files per request")
          .build();

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
  public Options getOptions() {
    return new Options().addOption(ACTIVE_JOB_COUNT_OPTION).addOption(OVERWRITE_OPTION)
        .addOption(BATCH_SIZE_OPTION).addOption(ASYNC_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public String getUsage() {
    return "distributedCp [--active-jobs <num>] [--batch-size <num>] <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or directory in parallel at file level.";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    mActiveJobs = FileSystemShellUtils.getIntArg(cl, ACTIVE_JOB_COUNT_OPTION,
        AbstractDistributedJobCommand.DEFAULT_ACTIVE_JOBS);
    boolean overwrite = FileSystemShellUtils.getBoolArg(cl, OVERWRITE_OPTION, true);
    boolean async = cl.hasOption(ASYNC_OPTION.getLongOpt());
    if (async) {
      System.out.println("Entering async submission mode. ");
    }

    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);
    AlluxioURI dstPath = new AlluxioURI(args[1]);

    if (PathUtils.hasPrefix(dstPath.toString(), srcPath.toString())) {
      throw new RuntimeException(
          ExceptionMessage.MIGRATE_CANNOT_BE_TO_SUBDIRECTORY.getMessage(srcPath, dstPath));
    }

    AlluxioConfiguration conf = mFsContext.getPathConf(dstPath);

    mWriteType = conf.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    int defaultBatchSize = conf.getInt(PropertyKey.JOB_REQUEST_BATCH_SIZE);
    int batchSize = FileSystemShellUtils.getIntArg(cl, BATCH_SIZE_OPTION, defaultBatchSize);
    System.out.println("Please wait for command submission to finish..");

    long jobControlId = distributedCp(srcPath, dstPath, overwrite, batchSize);
    if (!async) {
      System.out.format("Submitted successfully, jobControlId = %s%n"
              + "Waiting for the command to finish ...%n", jobControlId);
      waitForCmd(jobControlId);
      postProcessing(jobControlId);
    } else {
      System.out.format("Submitted migrate job successfully, jobControlId = %s%n",
              jobControlId);
    }

    Set<String> failures = getFailedFiles();
    if (failures.size() > 0) {
      processFailures(args[0], failures, DEFAULT_FAILURE_FILE_PATH);
    }

    return 0;
  }

  private long distributedCp(AlluxioURI srcPath, AlluxioURI dstPath,
        boolean overwrite, int batchSize) {
    CmdConfig cmdConfig = new MigrateCliConfig(srcPath.getPath(),
            dstPath.getPath(), mWriteType, overwrite, batchSize);
    return submit(cmdConfig);
  }
}
