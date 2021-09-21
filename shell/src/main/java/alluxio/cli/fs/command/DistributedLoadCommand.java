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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

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
        .addOption(INDEX_FILE)
        .addOption(HOSTS_OPTION)
        .addOption(HOST_FILE_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public String getUsage() {
    return "distributedLoad [--replication <num>] [--active-jobs <num>] [--index] "
        + "[--hosts <host1>,<host2>,...,<hostN>] [--host-file <hostFilePath>] "
        + "[--excluded-hosts <host1>,<host2>,...,<hostN>] [--excluded-host-file <hostFilePath>] "
        + "[--locality <locality1>,<locality2>,...,<localityN>] "
        + "[--locality-file <localityFilePath>] "
        + "[--excluded-locality <locality1>,<locality2>,...,<localityN>] "
        + "[--excluded-locality-file <localityFilePath>] "
        + "<path>";
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

    if (!cl.hasOption(INDEX_FILE.getLongOpt())) {
      AlluxioURI path = new AlluxioURI(args[0]);
      DistributedLoadUtils.distributedLoad(this, path, replication, workerSet,
          excludedWorkerSet, localityIds, excludedLocalityIds, true);
    } else {
      try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
        for (String filename; (filename = reader.readLine()) != null; ) {
          AlluxioURI path = new AlluxioURI(filename);
          DistributedLoadUtils.distributedLoad(this, path, replication, workerSet,
              excludedWorkerSet, localityIds, excludedLocalityIds, true);
        }
      }
    }
    System.out.println(String.format("Completed count is %d,Failed count is %d.",
        getCompletedCount(), getFailedCount()));
    return 0;
  }

  private void readItemsFromOptionString(Set<String> localityIds,
      String argOption) {
    for (String locality : StringUtils.split(argOption, ",")) {
      locality = locality.trim().toUpperCase();
      if (!locality.isEmpty()) {
        localityIds.add(locality);
      }
    }
  }

  private void readLinesToSet(Set<String> workerSet, String hostFile)
      throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(hostFile))) {
      for (String worker; (worker = reader.readLine()) != null; ) {
        worker = worker.trim().toUpperCase();
        if (!worker.isEmpty()) {
          workerSet.add(worker);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    mClient.close();
  }
}
