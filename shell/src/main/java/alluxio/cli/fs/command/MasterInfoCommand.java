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

import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
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
@PublicApi
public final class MasterInfoCommand extends AbstractFileSystemCommand {
  /**
   * @param fsContext the {@link FileSystem}
   */
  public MasterInfoCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "masterInfo";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) {
    MasterInquireClient inquireClient =
        MasterInquireClient.Factory
            .create(mFsContext.getClusterConf(), mFsContext.getClientContext().getUserState());
    try {
      InetSocketAddress leaderAddress = inquireClient.getPrimaryRpcAddress();
      System.out.println("Current leader master: " + leaderAddress.toString());
    } catch (UnavailableException e) {
      System.out.println("Failed to find leader master");
    }

    try {
      List<InetSocketAddress> masterAddresses = inquireClient.getMasterRpcAddresses();
      System.out.println(String.format("All masters: %s", masterAddresses));
    } catch (UnavailableException e) {
      System.out.println("Failed to find all master addresses");
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
