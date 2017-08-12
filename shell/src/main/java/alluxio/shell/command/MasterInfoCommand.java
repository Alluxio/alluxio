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
import alluxio.master.MasterInquireClient;

import org.apache.commons.cli.CommandLine;

import java.net.InetSocketAddress;
import java.util.List;

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
    MasterInquireClient inquireClient = MasterInquireClient.Factory.create();
    InetSocketAddress leaderAddress = inquireClient.getPrimaryRpcAddress();
    if (leaderAddress != null) {
      System.out.println("Current leader master: " + leaderAddress.toString());
    } else {
      System.out.println("Failed to find leader master");
    }
    List<InetSocketAddress> masterAddresses = inquireClient.getMasterRpcAddresses();
    if (masterAddresses != null) {
      System.out.println(String.format("All masters: %s", masterAddresses));
    }
    return 0;
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
