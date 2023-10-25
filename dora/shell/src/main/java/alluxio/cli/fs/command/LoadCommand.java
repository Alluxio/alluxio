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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadJobPOptions;
import alluxio.job.JobDescription;
import alluxio.job.LoadJobRequest;
import alluxio.util.FormatUtils;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in Alluxio.
 */
@ThreadSafe
@PublicApi
public final class LoadCommand extends AbstractFileSystemCommand {
  private static final JobProgressReportFormat DEFAULT_FORMAT = JobProgressReportFormat.TEXT;
  private static final String JOB_TYPE = "load";
  private static final Option SUBMIT_OPTION = Option.builder()
      .longOpt("submit")
      .required(false)
      .hasArg(false)
      .desc("Submit load job to Alluxio master, update job options if already exists.")
      .build();

  private static final Option STOP_OPTION = Option.builder()
      .longOpt("stop")
      .required(false)
      .hasArg(false)
      .desc("Stop a load job if it's still running.")
      .build();

  private static final Option PROGRESS_OPTION = Option.builder()
      .longOpt("progress")
      .required(false)
      .hasArg(false)
      .desc("Get progress report of a load job.")
      .build();

  private static final Option PARTIAL_LISTING_OPTION = Option.builder()
      .longOpt("partial-listing")
      .required(false)
      .hasArg(false)
      .desc("Use partial directory listing. This limits the memory usage "
          + "and starts load sooner for larger directory. But progress "
          + "report cannot report on the total number of files because the "
          + "whole directory is not listed yet.")
      .build();

  private static final Option VERIFY_OPTION = Option.builder()
      .longOpt("verify")
      .required(false)
      .hasArg(false)
      .desc("Run verification when load finish and load new files if any.")
      .build();

  private static final Option BANDWIDTH_OPTION = Option.builder()
      .longOpt("bandwidth")
      .required(false)
      .hasArg(true)
      .desc("Single worker read bandwidth limit.")
      .build();

  private static final Option PROGRESS_FORMAT = Option.builder()
      .longOpt("format")
      .required(false)
      .hasArg(true)
      .desc("Format of the progress report, supports TEXT and JSON. If not "
          + "set, TEXT is used.")
      .build();

  private static final Option PROGRESS_VERBOSE = Option.builder()
      .longOpt("verbose")
      .required(false)
      .hasArg(false)
      .desc("Whether to return a verbose progress report with detailed errors")
      .build();

  private static final Option LOAD_METADATA_ONLY = Option.builder()
      .longOpt("metadata-only")
      .required(false)
      .hasArg(false)
      .desc("If specified, only the file metadata are loaded")
      .build();

  private static final Option SKIP_IF_EXISTS = Option.builder()
      .longOpt("skip-if-exists")
      .required(false)
      .hasArg(false)
      .desc("If specified, skip files if they exist and are fully cached in alluxio.")
      .build();

  private static final Option FILE_FILTER_REGX = Option.builder()
      .longOpt("file-filter-regx")
      .required(false)
      .hasArg(true)
      .desc("If specified, skip files that doesn't match the regx pattern.")
      .build();

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LoadCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(BANDWIDTH_OPTION)
        .addOption(PARTIAL_LISTING_OPTION)
        .addOption(VERIFY_OPTION)
        .addOption(SUBMIT_OPTION)
        .addOption(STOP_OPTION)
        .addOption(PROGRESS_OPTION)
        .addOption(PROGRESS_FORMAT)
        .addOption(PROGRESS_VERBOSE)
        .addOption(LOAD_METADATA_ONLY)
        .addOption(SKIP_IF_EXISTS)
        .addOption(FILE_FILTER_REGX);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    if (path.containsWildcard()) {
      throw new UnsupportedOperationException("Load does not support wildcard path");
    }

    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      OptionalLong bandwidth = OptionalLong.empty();
      if (cl.hasOption(BANDWIDTH_OPTION.getLongOpt())) {
        bandwidth = OptionalLong.of(FormatUtils.parseSpaceSize(
            cl.getOptionValue(BANDWIDTH_OPTION.getLongOpt())));
      }
      Optional<String> regxPatternStr = Optional.empty();
      if (cl.hasOption(FILE_FILTER_REGX.getLongOpt())) {
        regxPatternStr = Optional.of(cl.getOptionValue(FILE_FILTER_REGX.getLongOpt()));
      }
      return submitLoad(
          path,
          bandwidth,
          cl.hasOption(PARTIAL_LISTING_OPTION.getLongOpt()),
          cl.hasOption(VERIFY_OPTION.getLongOpt()),
          cl.hasOption(LOAD_METADATA_ONLY.getLongOpt()),
          cl.hasOption(SKIP_IF_EXISTS.getLongOpt()),
          regxPatternStr);
    }

    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      return stopLoad(path);
    }
    JobProgressReportFormat format = DEFAULT_FORMAT;
    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      if (cl.hasOption(PROGRESS_FORMAT.getLongOpt())) {
        format = JobProgressReportFormat.valueOf(cl.getOptionValue(PROGRESS_FORMAT.getLongOpt()));
      }
      return getProgress(path, format, cl.hasOption(PROGRESS_VERBOSE.getLongOpt()));
    }

    return 0;
  }

  @Override
  public String getUsage() {
    return "For distributed load:\n"
        + "\tload <path> --submit "
        + "[--bandwidth N] [--verify] [--partial-listing] [--metadata-only] [--skip-if-exists] "
        + "[--file-filter-regx <regx_pattern_string>]\n"
        + "\tload <path> --stop\n"
        + "\tload <path> --progress [--format TEXT|JSON] [--verbose]\n";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in Alluxio.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
    int commands = 0;
    if (cl.hasOption(SUBMIT_OPTION.getLongOpt())) {
      commands++;
    }
    if (cl.hasOption(STOP_OPTION.getLongOpt())) {
      commands++;
    }
    if (cl.hasOption(PROGRESS_OPTION.getLongOpt())) {
      commands++;
    }
    if (commands != 1) {
      throw new InvalidArgumentException("Must have one of submit / stop / progress");
    }
  }

  private int submitLoad(AlluxioURI path, OptionalLong bandwidth,
      boolean usePartialListing, boolean verify, boolean loadMetadataOnly, boolean skipIfExists,
                         Optional<String> regxPatternStr) {
    LoadJobPOptions.Builder options = alluxio.grpc.LoadJobPOptions
        .newBuilder().setPartialListing(usePartialListing).setVerify(verify)
        .setLoadMetadataOnly(loadMetadataOnly)
        .setSkipIfExists(skipIfExists);
    if (bandwidth.isPresent()) {
      options.setBandwidth(bandwidth.getAsLong());
    }
    if (regxPatternStr.isPresent()) {
      options.setFileFilterRegx(regxPatternStr.get());
    }
    LoadJobRequest job = new LoadJobRequest(path.toString(), options.build());
    try {
      Optional<String> jobId = mFileSystem.submitJob(job);
      if (jobId.isPresent()) {
        System.out.printf("Load '%s' is successfully submitted. JobId: %s%n", path, jobId.get());
      } else {
        System.out.printf("Load already running for path '%s' %n", path);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println("Failed to submit load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  private int stopLoad(AlluxioURI path) {
    try {
      if (mFileSystem.stopJob(JobDescription
          .newBuilder()
          .setPath(path.toString())
          .setType(JOB_TYPE)
          .build())) {
        System.out.printf("Load '%s' is successfully stopped.%n", path);
      }
      else {
        System.out.printf("Cannot find load job for path %s, it might have already been "
            + "stopped or finished%n", path);
      }
      return 0;
    } catch (StatusRuntimeException e) {
      System.out.println("Failed to stop load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  private int getProgress(AlluxioURI path, JobProgressReportFormat format,
      boolean verbose) {
    try {
      System.out.println("Progress for loading path '" + path + "':");
      System.out.println(mFileSystem.getJobProgress(JobDescription
          .newBuilder()
          .setPath(path.toString())
          .setType(JOB_TYPE)
          .build(), format, verbose));
      return 0;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        System.out.println("Load for path '" + path + "' cannot be found.");
        return -2;
      }
      System.out.println("Failed to get progress for load job " + path + ": " + e.getMessage());
      return -1;
    }
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    // TODO(jiacheng): refactor LoadCommand so the main logic is executed here
    throw new IllegalStateException("Should not reach here!");
  }
}
