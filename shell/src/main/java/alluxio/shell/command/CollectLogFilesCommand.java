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

package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.AlluxioException;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

public class CollectLogFilesCommand extends AbstractShellCommand {
  private static final Option COMPRESSION_OPTION =
      Option.builder("c").required(false).hasArg(false)
      .desc("perform compression on log files").build();

  public CollectLogFilesCommand(FileSystem fs) { super(fs); }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public String getCommandName() {
    return "collectLogFiles";
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    System.out.println("y7jin: collect log files");
    for (String s : args) {
      System.out.println(s);
    }
    try(CloseableResource<FileSystemMasterClient> client =
            FileSystemContext.INSTANCE.acquireMasterClientResource()) {
      client.get().collectLogFiles();
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(COMPRESSION_OPTION);
  }

  @Override
  public String getUsage() {
    return "collectLogFiles [-c]";
  }

  @Override
  public String getDescription() {
    return "Collect the log files on Alluxio servers";
  }
}
