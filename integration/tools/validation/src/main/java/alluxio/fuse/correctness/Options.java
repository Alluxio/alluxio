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

package alluxio.fuse.correctness;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

/**
 * This class stores the options passed in from CLI.
 */
public final class Options {
  public static final CommandLineParser PARSER = new DefaultParser();
  private final String mTmpDir;
  private final String mFuseDir;
  private final IOOperation mOperation;
  private final int mNumClients;
  private final int mNumFiles;
  private final int mTimeout;

  /**
   * Constructs a {@Options} object through CLI.
   *
   * @param args arguments from CLI
   * @return a {@Options} holding all the test options
   */
  public static Options createOptions(String[] args) {
    CommandLine cli;
    try {
      cli = PARSER.parse(CorrectnessOptionsParser.OPTIONS, args);
    } catch (ParseException e) {
      throw new RuntimeException("Error parsing command line input.", e);
    }
    String fuseDir = cli.getOptionValue(CorrectnessOptionsParser.FUSE_DIR_OPTION_NAME);
    String localDir = cli.hasOption(CorrectnessOptionsParser.TMP_DIR_OPTION_NAME)
        ? cli.getOptionValue(CorrectnessOptionsParser.TMP_DIR_OPTION_NAME)
        : Constants.DEFAULT_LOCAL_DIR;
    IOOperation operation = IOOperation.fromString(
        cli.hasOption(CorrectnessOptionsParser.TEST_OPERATION_OPTION_NAME)
            ? cli.getOptionValue(CorrectnessOptionsParser.TEST_OPERATION_OPTION_NAME)
            : Constants.DEFAULT_OPERATION);
    int numThreads = Integer.parseInt(
        cli.hasOption(CorrectnessOptionsParser.CLIENT_NUMBER_OPTION_NAME)
            ? cli.getOptionValue(CorrectnessOptionsParser.CLIENT_NUMBER_OPTION_NAME)
            : Constants.DEFAULT_NUM_THREADS);
    int numFiles = Integer.parseInt(
        cli.hasOption(CorrectnessOptionsParser.FILE_NUMBER_OPTION_NAME)
            ? cli.getOptionValue(CorrectnessOptionsParser.FILE_NUMBER_OPTION_NAME)
            : Constants.DEFAULT_NUM_FILES);
    int timeout = Integer.parseInt(
        cli.hasOption(CorrectnessOptionsParser.TEST_TIMEOUT_OPTION_NAME)
            ? cli.getOptionValue(CorrectnessOptionsParser.TEST_TIMEOUT_OPTION_NAME)
            : Constants.DEFAULT_TIMEOUT);

    Options options =  new Options(localDir, fuseDir, operation, numThreads, numFiles, timeout);
    validateOptions(options);
    return options;
  }

  private static void validateOptions(Options opts) {
    if (opts.getNumFiles() <= 0) {
      throw new IllegalArgumentException(
          "Number of files for validation must be positive. "
              + "Please provide a valid number for number of files.");
    }
    if (opts.getNumThreads() <= 0) {
      throw new IllegalArgumentException(
          "Number of threads for validation must be positive. "
              + "Please provide a valid number for number of threads.");
    }
    if (opts.getOperation() == IOOperation.Write && opts.getNumThreads() != opts.getNumFiles()) {
      throw new IllegalArgumentException(
          "For writing test, the number of threads must be equal to the number of files, because "
              + "AlluxioFuse does not support multiple threads writing to the same file. "
              + "The test is stopped.");
    }
  }

  private Options(String tmpDir, String fuseDir,
      IOOperation operation, int numClients, int numFiles, int timeout) {
    mTmpDir = Preconditions.checkNotNull(tmpDir);
    mFuseDir = Preconditions.checkNotNull(fuseDir);
    mOperation = Preconditions.checkNotNull(operation);
    mNumClients = numClients;
    mNumFiles = numFiles;
    mTimeout = timeout;
  }

  /**
   * @return operation being tested
   */
  public IOOperation getOperation() {
    return mOperation;
  }

  /**
   * @return number of threads for the test
   */
  public int getNumThreads() {
    return mNumClients;
  }

  /**
   * @return number of files for the test
   */
  public int getNumFiles() {
    return mNumFiles;
  }

  /**
   * @return the local filesystem directory for testing
   */
  public String getLocalDir() {
    return mTmpDir;
  }

  /**
   * @return the fuse directory for testing
   */
  public String getFuseDir() {
    return mFuseDir;
  }

  /**
   * @return the timeout for the test
   */
  public int getTimeout() {
    return mTimeout;
  }

  private static class CorrectnessOptionsParser {
    private static final String TMP_DIR_OPTION_NAME = "t";
    private static final String FUSE_DIR_OPTION_NAME = "f";
    private static final String FILE_NUMBER_OPTION_NAME = "n";
    private static final String CLIENT_NUMBER_OPTION_NAME = "c";
    private static final String TEST_OPERATION_OPTION_NAME = "o";
    private static final String TEST_TIMEOUT_OPTION_NAME = "d";
    private static final String HELP_OPTION_NAME = "h";

    private static final Option FUSE_DIR_OPTION =
        Option.builder(FUSE_DIR_OPTION_NAME)
            .required(true)
            .hasArg()
            .longOpt("fuse-dir")
            .desc("The directory managed by Alluxio Fuse to write test file"
                + " for validating correctness.")
            .build();
    private static final Option TMP_DIR_OPTION =
        Option.builder(TMP_DIR_OPTION_NAME)
            .required(false)
            .hasArg()
            .longOpt("tmp-dir")
            .desc("The local filesystem directory to write source file"
                + " which is used for validating correctness.")
            .build();
    private static final Option FILE_NUMBER_OPTION =
        Option.builder(FILE_NUMBER_OPTION_NAME)
            .required(false)
            .hasArg()
            .longOpt("num-files")
            .desc("Number of files generated for validating")
            .build();
    private static final Option TEST_OPERATION_OPTION =
        Option.builder(TEST_OPERATION_OPTION_NAME)
            .required(false)
            .hasArg()
            .longOpt("operation")
            .desc("Operation being tested. Valid options include `Read` and `Write`.")
            .build();
    private static final Option CLIENT_NUMBER_OPTION =
        Option.builder(CLIENT_NUMBER_OPTION_NAME)
            .required(false)
            .hasArg()
            .longOpt("num-clients")
            .desc("Number of clients (threads) doing IO operations to the fuse mount point.")
            .build();
    private static final Option TEST_TIMEOUT_OPTION =
        Option.builder(TEST_TIMEOUT_OPTION_NAME)
            .required(false)
            .hasArg()
            .longOpt("timeout")
            .desc("The timeout period for the test in seconds."
                + " Only valid for multi-file Read validation.")
            .build();
    private static final Option HELP_OPTION =
        Option.builder(HELP_OPTION_NAME)
            .required(false)
            .longOpt("help")
            .desc("Print this help message.")
            .build();

    public static final org.apache.commons.cli.Options OPTIONS =
        new org.apache.commons.cli.Options()
            .addOption(TMP_DIR_OPTION)
            .addOption(FUSE_DIR_OPTION)
            .addOption(FILE_NUMBER_OPTION)
            .addOption(TEST_OPERATION_OPTION)
            .addOption(CLIENT_NUMBER_OPTION)
            .addOption(TEST_TIMEOUT_OPTION)
            .addOption(HELP_OPTION);
  }
}
