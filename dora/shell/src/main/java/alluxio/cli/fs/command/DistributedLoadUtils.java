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
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.job.CmdConfig;
import alluxio.job.cmd.load.LoadCliConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Utilities Loads a file or directory in Alluxio space, makes it resident in memory.
 */
public final class DistributedLoadUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedLoadUtils.class);
  private static final int DEFAULT_REPLICATION = 1;
  private static final String DEFAULT_FAILURE_FILE_PATH =
      "./logs/user/distributedLoad_%s_failures.csv";
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
  private static final Option HOSTS_OPTION =
      Option.builder()
          .longOpt("hosts")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("hosts")
          .desc("A list of worker hosts separated by comma."
              + " When host and locality options are not set,"
              + " all hosts will be selected unless explicitly excluded by setting excluded option"
              + "('excluded-hosts', 'excluded-host-file', 'excluded-locality'"
              + " and 'excluded-locality-file')."
              + " Only one of the 'hosts' and 'host-file' should be set,"
              + " and it should not be set with excluded option together.")
          .build();
  private static final Option HOST_FILE_OPTION =
      Option.builder()
          .longOpt("host-file")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("host-file")
          .desc("Host File contains worker hosts, each line has a worker host."
              + " When host and locality options are not set,"
              + " all hosts will be selected unless explicitly excluded by setting excluded option"
              + "('excluded-hosts', 'excluded-host-file', 'excluded-locality'"
              + " and 'excluded-locality-file')."
              + " Only one of the 'hosts' and 'host-file' should be set,"
              + " and it should not be set with excluded option together.")
          .build();
  private static final Option EXCLUDED_HOSTS_OPTION =
      Option.builder()
          .longOpt("excluded-hosts")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-hosts")
          .desc("A list of excluded worker hosts separated by comma."
              + " Only one of the 'excluded-hosts' and 'excluded-host-file' should be set,"
              + " and it should not be set with 'hosts', 'host-file', 'locality'"
              + " and 'locality-file' together.")
          .build();
  private static final Option EXCLUDED_HOST_FILE_OPTION =
      Option.builder()
          .longOpt("excluded-host-file")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-host-file")
          .desc("Host File contains excluded worker hosts, each line has a worker host."
              + " Only one of the 'excluded-hosts' and 'excluded-host-file' should be set,"
              + " and it should not be set with 'hosts', 'host-file', 'locality'"
              + " and 'locality-file' together.")
          .build();
  private static final Option LOCALITY_OPTION =
      Option.builder()
          .longOpt("locality")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("locality")
          .desc("A list of worker locality separated by comma."
              + " When host and locality options are not set,"
              + " all hosts will be selected unless explicitly excluded by setting excluded option"
              + "('excluded-hosts', 'excluded-host-file', 'excluded-locality'"
              + " and 'excluded-locality-file')."
              + " Only one of the 'locality' and 'locality-file' should be set,"
              + " and it should not be set with excluded option together.")
          .build();
  private static final Option LOCALITY_FILE_OPTION =
      Option.builder()
          .longOpt("locality-file")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("locality-file")
          .argName("locality-file")
          .desc("Locality File contains worker localities, each line has a worker locality."
              + " When host and locality options are not set,"
              + " all hosts will be selected unless explicitly excluded by setting excluded option"
              + "('excluded-hosts', 'excluded-host-file', 'excluded-locality'"
              + " and 'excluded-locality-file')."
              + " Only one of the 'locality' and 'locality-file' should be set,"
              + " and it should not be set with excluded option together.")
          .build();
  private static final Option EXCLUDED_LOCALITY_OPTION =
      Option.builder()
          .longOpt("excluded-locality")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-locality")
          .desc("A list of excluded worker locality separated by comma."
              + " Only one of the 'excluded-locality' and 'excluded-locality-file' should be set,"
              + " and it should not be set with 'hosts', 'host-file', 'locality'"
              + " and 'locality-file' together.")
          .build();
  private static final Option EXCLUDED_LOCALITY_FILE_OPTION =
      Option.builder()
          .longOpt("excluded-locality-file")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("excluded-locality-file")
          .desc("Locality File contains excluded worker localities,"
              + " each line has a worker locality."
              + " Only one of the 'excluded-locality' and 'excluded-locality-file' should be set,"
              + " and it should not be set with 'hosts', 'host-file', 'locality'"
              + " and 'locality-file' together.")
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
  private static final Option PASSIVE_CACHE_OPTION =
      Option.builder()
          .longOpt("passive-cache")
          .required(false)
          .hasArg(false)
          .desc("Use passive-cache as the cache implementation,"
              + " turn on to use the old cache through read implementation. "
              + "Passive-cache is default when there's no option set or "
              + "both options are set for cache implementation."
              + "Notice that this flag is temporary, "
              + "and it would retire after direct cache graduate from experimental stage")
          .build();
  private static final Option DIRECT_CACHE_OPTION =
      Option.builder()
          .longOpt("direct-cache")
          .required(false)
          .hasArg(false)
          .desc("Use direct cache request as the cache implementation,"
              + " turn on to use the new cache through cache manager implementation. "
              + "Notice that this flag is temporary, "
              + "and it would retire after direct cache graduate from experimental stage")
          .build();

  private DistributedLoadUtils() {} // prevent instantiation

  /**
   * Get the general options that distributed load needs to use.
   * Suitable for file semantics and structured data semantics.
   * @return general options
   */
  public static Options getDistLoadCommonOptions() {
    return new Options()
        .addOption(REPLICATION_OPTION)
        .addOption(ACTIVE_JOB_COUNT_OPTION)
        .addOption(HOSTS_OPTION)
        .addOption(HOST_FILE_OPTION)
        .addOption(EXCLUDED_HOSTS_OPTION)
        .addOption(EXCLUDED_HOST_FILE_OPTION)
        .addOption(LOCALITY_OPTION)
        .addOption(LOCALITY_FILE_OPTION)
        .addOption(EXCLUDED_LOCALITY_OPTION)
        .addOption(EXCLUDED_LOCALITY_FILE_OPTION)
        .addOption(PASSIVE_CACHE_OPTION)
        .addOption(DIRECT_CACHE_OPTION)
        .addOption(BATCH_SIZE_OPTION)
        .addOption(AbstractDistributedJobCommand.ASYNC_OPTION);
  }

  /**
   * Get the options of distributed load needs to use in file semantics.
   * @return options in file semantics
   */
  public static Options getDistLoadFileSemanticsOptions() {
    return getDistLoadCommonOptions()
        .addOption(INDEX_FILE);
  }

  /**
   * Get the general usage of distributed load.
   * Suitable for file semantics and structured data semantics.
   * @return usage message
   */
  public static String getDistLoadCommonUsage() {
    return "[--replication <num>] [--active-jobs <num>] [--batch-size <num>] "
        + "[--host-file <hostFilePath>] "
        + "[--excluded-hosts <host1>,<host2>,...,<hostN>] [--excluded-host-file <hostFilePath>] "
        + "[--locality <locality1>,<locality2>,...,<localityN>] "
        + "[--locality-file <localityFilePath>] "
        + "[--excluded-locality <locality1>,<locality2>,...,<localityN>] "
        + "[--excluded-locality-file <localityFilePath>] "
        + "[--passive-cache] "
        + "[--direct-cache] ";
  }

  /**
   * Get usage of distributed load in file semantics.
   * @return usage message
   */
  public static String getDistLoadFileSemanticsUsage() {
    return getDistLoadCommonUsage()
        + "[--index] [--hosts <host1>,<host2>,...,<hostN>] ";
  }

  /**
   * Run the actual distributedLoad command.
   * @param command Distributed Load command instance
   * @param cl CommandLine instance of command
   * @param pathStr path to be loaded
   * @return the result of running the command
   */
  public static int distributedLoad(AbstractDistributedJobCommand command, CommandLine cl,
                                     String pathStr)
      throws AlluxioException, IOException  {
    AlluxioConfiguration conf = command.mFsContext.getClusterConf();
    int defaultBatchSize = conf.getInt(PropertyKey.JOB_REQUEST_BATCH_SIZE);
    int replication = FileSystemShellUtils.getIntArg(cl, REPLICATION_OPTION, DEFAULT_REPLICATION);
    int batchSize = FileSystemShellUtils.getIntArg(cl, BATCH_SIZE_OPTION, defaultBatchSize);
    boolean directCache = !cl.hasOption(PASSIVE_CACHE_OPTION.getLongOpt()) && cl.hasOption(
        DIRECT_CACHE_OPTION.getLongOpt());
    boolean async = cl.hasOption(AbstractDistributedJobCommand.ASYNC_OPTION.getLongOpt());
    if (async) {
      System.out.println("Entering async submission mode. ");
    }

    Set<String> workerSet = new HashSet<>();
    Set<String> excludedWorkerSet = new HashSet<>();
    Set<String> localityIds = new HashSet<>();
    Set<String> excludedLocalityIds = new HashSet<>();
    if (cl.hasOption(HOST_FILE_OPTION.getLongOpt())) {
      String hostFile = cl.getOptionValue(HOST_FILE_OPTION.getLongOpt()).trim();
      readLinesToSet(workerSet, hostFile);
    } else if (cl.hasOption(HOSTS_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(HOSTS_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(workerSet, argOption);
    }
    if (cl.hasOption(EXCLUDED_HOST_FILE_OPTION.getLongOpt())) {
      String hostFile = cl.getOptionValue(EXCLUDED_HOST_FILE_OPTION.getLongOpt()).trim();
      readLinesToSet(excludedWorkerSet, hostFile);
    } else if (cl.hasOption(EXCLUDED_HOSTS_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(EXCLUDED_HOSTS_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(excludedWorkerSet, argOption);
    }
    if (cl.hasOption(LOCALITY_FILE_OPTION.getLongOpt())) {
      String localityFile = cl.getOptionValue(LOCALITY_FILE_OPTION.getLongOpt()).trim();
      readLinesToSet(localityIds, localityFile);
    } else if (cl.hasOption(LOCALITY_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(LOCALITY_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(localityIds, argOption);
    }
    if (cl.hasOption(EXCLUDED_LOCALITY_FILE_OPTION.getLongOpt())) {
      String localityFile = cl.getOptionValue(EXCLUDED_LOCALITY_FILE_OPTION.getLongOpt()).trim();
      readLinesToSet(excludedLocalityIds, localityFile);
    } else if (cl.hasOption(EXCLUDED_LOCALITY_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(EXCLUDED_LOCALITY_OPTION.getLongOpt()).trim();
      readItemsFromOptionString(excludedLocalityIds, argOption);
    }

    System.out.println("Please wait for command submission to finish..");

    long jobControlId;
    if (!cl.hasOption(INDEX_FILE.getLongOpt())) {
      AlluxioURI path = new AlluxioURI(pathStr);
      jobControlId = DistributedLoadUtils.runDistLoad(command, path, replication, batchSize,
          workerSet, excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
      if (!async) {
        System.out.format("Submitted successfully, jobControlId = %s%n"
            + "Waiting for the command to finish ...%n", jobControlId);
        command.waitForCmd(jobControlId);
        command.postProcessing(jobControlId);
      } else {
        System.out.format("Submitted distLoad job successfully, jobControlId = %s%n",
            jobControlId);
      }
    } else {
      try (BufferedReader reader = new BufferedReader(new FileReader(pathStr))) {
        for (String filename; (filename = reader.readLine()) != null;) {
          AlluxioURI path = new AlluxioURI(filename);
          jobControlId = DistributedLoadUtils.runDistLoad(command, path, replication, batchSize,
              workerSet, excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
          if (!async) {
            System.out.format("Submitted successfully, jobControlId = %s%n"
                + "Waiting for the command to finish ...%n", jobControlId);
            command.waitForCmd(jobControlId);
            command.postProcessing(jobControlId);
          } else {
            System.out.format("Submitted distLoad job successfully, jobControlId = %s%n",
                jobControlId);
          }
        }
      }
    }

    Set<String> failures = command.getFailedFiles();
    if (failures.size() > 0) {
      command.processFailures(pathStr, failures, DEFAULT_FAILURE_FILE_PATH);
    }
    return 0;
  }

  /**
   * Run the actual distributedLoad command.
   * @param command Distributed Load command instance
   * @param filePath file path to load
   * @param replication Number of block replicas of each loaded file
   * @param batchSize Batch size for loading
   * @param workerSet A set of worker hosts to load data
   * @param excludedWorkerSet A set of worker hosts can not to load data
   * @param localityIds The locality identify set
   * @param excludedLocalityIds A set of worker locality identify can not to load data
   * @param directCache use direct cache request or cache through read
   * @return job Control ID
   */
  public static long runDistLoad(AbstractDistributedJobCommand command, AlluxioURI filePath,
                                     int replication, int batchSize, Set<String> workerSet,
                                     Set<String> excludedWorkerSet, Set<String> localityIds,
                                     Set<String> excludedLocalityIds, boolean directCache) {
    CmdConfig cmdConfig = new LoadCliConfig(filePath.getPath(), batchSize, replication, workerSet,
        excludedWorkerSet, localityIds, excludedLocalityIds, directCache);
    return command.submit(cmdConfig);
  }

  private static void readItemsFromOptionString(Set<String> localityIds, String argOption) {
    for (String locality : StringUtils.split(argOption, ",")) {
      locality = locality.trim().toUpperCase();
      if (!locality.isEmpty()) {
        localityIds.add(locality);
      }
    }
  }

  private static void readLinesToSet(Set<String> workerSet, String hostFile) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(hostFile))) {
      for (String worker; (worker = reader.readLine()) != null;) {
        worker = worker.trim().toUpperCase();
        if (!worker.isEmpty()) {
          workerSet.add(worker);
        }
      }
    }
  }
}
