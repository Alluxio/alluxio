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
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobThriftClientUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.load.LoadConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
public final class DistributedLoadCommand extends AbstractFileSystemCommand {
  private static final String REPLICATION = "replication";

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fs the filesystem of Alluxio
   */
  public DistributedLoadCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "distributedLoad";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(Option.builder(REPLICATION)
        .required(false)
        .hasArg(true)
        .desc("number of replicas to have for each block of the loaded file")
        .build());
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    int replication = 1;
    if (cl.hasOption(REPLICATION)) {
      replication = Integer.parseInt(cl.getOptionValue(REPLICATION));
    }
    try {
      load(path, replication);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    }
    return 0;
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int replication)
      throws AlluxioException, IOException, InterruptedException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath, replication);
      }
    } else {
      Thread thread = JobThriftClientUtils.createProgressThread(System.out);
      thread.start();
      try {
        JobThriftClientUtils.run(new LoadConfig(filePath.getPath(), replication), 3);
      } finally {
        thread.interrupt();
      }
    }
    System.out.println(filePath + " loaded");
  }

  @Override
  public String getUsage() {
    return "distributedLoad [-replication N] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, making it resident in memory.";
  }
}
