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

import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current master leader host name.
 */
@ThreadSafe
public final class LeaderCommand extends AbstractShellCommand {
  /**
   * @param conf the configuration of Alluxio
   * @param fs the filesystem of Alluxio
   */
  public LeaderCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "leader";
  }

  @Override
  public int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(CommandLine cl) {
    FileSystemMasterClient client = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      String hostName = client.getAddress().getHostName();
      if (hostName != null) {
        System.out.println(hostName);
      } else {
        System.out.println("Failed to get the master leader.");
      }
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(client);
    }
  }

  @Override
  public String getUsage() {
    return "leader";
  }

  @Override
  public String getDescription() {
    return "Prints the current master leader host name.";
  }
}
