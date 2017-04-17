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
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current leader master host name.
 */
@ThreadSafe
public final class LeaderCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public LeaderCommand(FileSystem fs) {
    super(fs);
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
  public int run(CommandLine cl) {
    try (CloseableResource<FileSystemMasterClient> client =
        FileSystemContext.INSTANCE.acquireMasterClientResource()) {
      String hostName = client.get().getAddress().getHostName();
      if (hostName != null) {
        System.out.println(hostName);
      } else {
        System.out.println("Failed to get the leader master.");
      }
    }
    return 0;
  }

  @Override
  public String getUsage() {
    return "leader";
  }

  @Override
  public String getDescription() {
    return "Prints the current leader master host name.";
  }
}
