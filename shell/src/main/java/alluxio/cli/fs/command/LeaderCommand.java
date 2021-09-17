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
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;

import org.apache.commons.cli.CommandLine;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the current leader master host name.
 *
 * @deprecated This command will be deprecated as of v3.0, use {@link MasterInfoCommand}
 */
@ThreadSafe
@PublicApi
@Deprecated
public final class LeaderCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public LeaderCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "leader";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) {
    System.out.println("This command will be deprecated as of v3.0, please use masterInfo command");
    try (CloseableResource<FileSystemMasterClient> client =
        mFsContext.acquireMasterClientResource()) {
      try {
        InetSocketAddress address = client.get().getAddress();
        System.out.println(address.getHostName());

        List<InetSocketAddress> addresses = Arrays.asList(address);
        MasterInquireClient inquireClient = new PollingMasterInquireClient(addresses, () ->
                new ExponentialBackoffRetry(50, 100, 2), mFsContext.getClusterConf()
        );
        try {
          inquireClient.getPrimaryRpcAddress();
        } catch (UnavailableException e) {
          System.err.println("The leader is not currently serving requests.");
        }
      } catch (UnavailableException e) {
        System.err.println("Failed to get the leader master.");
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
