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
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
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
          .desc("A list of worker hosts separated by comma")
          .build();
  private static final Option HOST_FILE_OPTION =
      Option.builder()
          .longOpt("host-file")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("host-file")
          .desc("Host File contains worker hosts, every line has a worker host")
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
        + "[--hosts <host1,host2,...,hostn>] [--host-file <hostFilePath>] <path>";
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
    Set<String> workerSet = Collections.EMPTY_SET;
    if (cl.hasOption(HOST_FILE_OPTION.getLongOpt())) {
      String hostFile = cl.getOptionValue(HOST_FILE_OPTION.getLongOpt()).trim();
      try (BufferedReader reader = new BufferedReader(new FileReader(hostFile))) {
        workerSet = new HashSet<String>();
        for (String worker; (worker = reader.readLine()) != null; ) {
          worker = worker.trim();
          if (!worker.isEmpty()) {
            workerSet.add(worker.toUpperCase());
          }
        }
      }
    } else if (cl.hasOption(HOSTS_OPTION.getLongOpt())) {
      String argOption = cl.getOptionValue(HOSTS_OPTION.getLongOpt()).trim();
      workerSet = Arrays.stream(StringUtils.split(argOption, ","))
          .map(str -> str.trim().toUpperCase())
          .collect(Collectors.toSet());
    }

    if (!cl.hasOption(INDEX_FILE.getLongOpt())) {
      AlluxioURI path = new AlluxioURI(args[0]);
      DistributedLoadUtils.distributedLoad(this, path, replication, workerSet);
    } else {
      try (BufferedReader reader = new BufferedReader(new FileReader(args[0]))) {
        for (String filename; (filename = reader.readLine()) != null; ) {
          AlluxioURI path = new AlluxioURI(filename);
          DistributedLoadUtils.distributedLoad(this, path, replication, workerSet);
        }
      }
    }
    return 0;
  }

  @Override
  public void close() throws IOException {
    mClient.close();
  }
}
