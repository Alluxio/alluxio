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
import alluxio.PropertyKey;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.resource.CloseableResource;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints information regarding master fault tolerance such as leader address, list of master
 * addresses, and the configured Zookeeper address.
 */
@ThreadSafe
public final class MasterInfoCommand extends AbstractShellCommand {
  /**
   * @param fs the {@link FileSystem}
   */
  public MasterInfoCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "masterInfo";
  }

  @Override
  public int getNumOfArgs() {
    return 0;
  }

  @Override
  public int run(CommandLine cl) {
    try (CloseableResource<FileSystemMasterClient> client =
        FileSystemContext.INSTANCE.acquireMasterClientResource()) {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        runWithFaultTolerance(client.get());
      } else {
        runWithoutFaultTolerance(client.get());
      }
    }
    return 0;
  }

  private void runWithoutFaultTolerance(FileSystemMasterClient client) {
    System.out.println("Alluxio cluster without fault tolerance now");
    printLeader(client);
  }

  private void runWithFaultTolerance(FileSystemMasterClient client) {
    System.out.println("Alluxio cluster with fault tolerance now");
    printLeader(client);
    System.out.println(String.format("All masters: %s", client.getMasterAddresses()));
    System.out.println(String.format("Zookeeper address: %s",
        Configuration.get(PropertyKey.ZOOKEEPER_ADDRESS)));
  }

  private void printLeader(FileSystemMasterClient client) {
    String hostName = client.getAddress().getHostName();
    if (hostName != null) {
      System.out.println(String.format("Current leader master: %s", hostName));
    } else {
      System.out.println("Failed to get the current leader master.");
    }
  }

  @Override
  public String getUsage() {
    return "masterInfo";
  }

  @Override
  public String getDescription() {
    return "Prints information regarding master fault tolerance such as leader address, list of "
        + "master addresses, and the configured Zookeeper address.";
  }
}
