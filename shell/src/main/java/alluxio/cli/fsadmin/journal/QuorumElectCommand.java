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

package alluxio.cli.fsadmin.journal;

import static alluxio.master.journal.raft.RaftJournalSystem.RAFT_GROUP_ID;

import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.GetQuorumInfoPResponse;
import alluxio.grpc.NetAddress;
import alluxio.grpc.QuorumServerInfo;
import alluxio.master.journal.raft.RaftJournalConfiguration;
import alluxio.master.journal.raft.RaftJournalUtils;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.ExponentialBackoffRetry;
import org.apache.ratis.util.TimeDuration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Command for transferring the leadership to another master within a quorum.
 */
public class QuorumElectCommand extends AbstractFsAdminCommand {

  public static final String ADDRESS_OPTION_NAME = "address";

  public static final String TRANSFER_SUCCESS = "Successfully elected %s as the new leader";
  public static final String TRANSFER_FAILED = "Failed to elect %s as the new leader: %s";

  private final ClientId mRawClientId = ClientId.randomId();
  private final RaftJournalConfiguration mRaftJournalConf;
  private final RaftGroup mRaftGroup;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public QuorumElectCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mRaftJournalConf =
        RaftJournalConfiguration.defaults(
            NetworkAddressUtils.ServiceType.MASTER_RAFT);
    List<InetSocketAddress> addresses = mRaftJournalConf.getClusterAddresses();
    Set<RaftPeer> peers = addresses.stream()
        .map(addr -> RaftPeer.newBuilder()
            .setId(RaftJournalUtils.getPeerId(addr))
            .setAddress(addr)
            .build()
        )
        .collect(Collectors.toSet());
    mRaftGroup = RaftGroup.valueOf(RAFT_GROUP_ID, peers);
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Transfers leadership of the quorum to the master located at <hostname>:<port>";
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String strAddr = cl.getOptionValue(ADDRESS_OPTION_NAME);
    NetAddress address = QuorumCommand.stringToAddress(strAddr);
    InetSocketAddress serverAddress = InetSocketAddress
        .createUnresolved(address.getHost(), address.getRpcPort());
    List<RaftPeer> oldPeers = new ArrayList<>(mRaftGroup.getPeers());
    // if you cannot find the address in the quorum, throw exception.
    if (oldPeers.stream().map(RaftPeer::getAddress).noneMatch(addr -> addr.equals(strAddr))) {
      throw new IOException(String.format("<%s> is not part of the quorum <%s>.",
          strAddr, oldPeers.stream().map(RaftPeer::getAddress).collect(Collectors.toList())));
    }

    RaftPeerId newLeaderPeerId =
        RaftJournalUtils.getPeerId(address.getHost(), address.getRpcPort());
    /* update priorities to enable transfer */
    List<RaftPeer> peersWithNewPriorities = new ArrayList<>();
    for (RaftPeer peer : oldPeers) {
      peersWithNewPriorities.add(
          RaftPeer.newBuilder(peer)
              .setPriority(peer.getId().equals(newLeaderPeerId) ? 2 : 1)
              .build()
      );
    }
    try (RaftClient client = createClient()) {
      String stringPeers = "[" + peersWithNewPriorities.stream().map(RaftPeer::toString)
          .collect(Collectors.joining(", ")) + "]";
      mPrintStream.printf(
          "Applying new peer state before transferring leadership: %s%n", stringPeers);
      RaftClientReply reply = client.admin().setConfiguration(peersWithNewPriorities);
      processReply(reply, "failed to set master priorities before initiating election");
      /* transfer leadership */
      mPrintStream.printf(
          "Transferring leadership to master with address <%s> and with RaftPeerId <%s>%n",
          serverAddress, newLeaderPeerId);
      // fire and forget: need to immediately return as the master will shut down its RPC servers
      // once the TransferLeadershipRequest is initiated.
      final int SLEEP_TIME_MS = 3_000;
      final int TRANSFER_LEADER_WAIT_MS = 60_000;
      try {
        Thread.sleep(SLEEP_TIME_MS);
        RaftClientReply reply1 = client.admin().transferLeadership(newLeaderPeerId,
            TRANSFER_LEADER_WAIT_MS);
        processReply(reply1, "election failed");
      } catch (Throwable t) {
        mPrintStream.printf("caught an error when executing transfer: %s%n", t.getMessage());
        return -1;
      }
      mPrintStream.println("Transferring leadership initiated");
    } catch (Throwable t) {
      throw new IOException(t);
    }

    try {
      // wait for confirmation of leadership transfer
      final int TIMEOUT_3MIN = 3 * 60 * 1000; // in milliseconds
      CommonUtils.waitFor("Waiting for election to finalize", () -> {
        try {
          GetQuorumInfoPResponse quorumInfo = mMasterJournalMasterClient.getQuorumInfo();

          Optional<QuorumServerInfo>
              leadingMasterInfoOpt = quorumInfo.getServerInfoList().stream()
              .filter(QuorumServerInfo::getIsLeader).findFirst();
          NetAddress leaderAddress = leadingMasterInfoOpt.isPresent()
              ? leadingMasterInfoOpt.get().getServerAddress() : null;
          return address.equals(leaderAddress);
        } catch (AlluxioStatusException e) {
          return false;
        }
      }, WaitForOptions.defaults().setTimeoutMs(TIMEOUT_3MIN));

      mPrintStream.println(String.format(TRANSFER_SUCCESS, serverAddress));
      return 0;
    } catch (InterruptedException | TimeoutException e) {
      mPrintStream.println(String.format(TRANSFER_FAILED, serverAddress, "the election was "
              + "initiated but never completed"));
    }
    return -1;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    if (!cl.hasOption(ADDRESS_OPTION_NAME)) {
      throw new InvalidArgumentException(ExceptionMessage.INVALID_OPTION
              .getMessage(String.format("[%s]", ADDRESS_OPTION_NAME)));
    }
  }

  @Override
  public String getCommandName() {
    return "elect";
  }

  @Override
  public String getUsage() {
    return String.format("%s -%s <HOSTNAME:PORT>", getCommandName(), ADDRESS_OPTION_NAME);
  }

  @Override
  public String getDescription() {
    return description();
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESS_OPTION_NAME, true,
            "Server address that will take over as leader");
  }

  private RaftClient createClient() {
    RaftProperties properties = new RaftProperties();
    Parameters parameters = new Parameters();
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
        TimeDuration.valueOf(15, TimeUnit.SECONDS));
    ExponentialBackoffRetry retryPolicy = ExponentialBackoffRetry.newBuilder()
        .setBaseSleepTime(TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))
        .setMaxAttempts(10)
        .setMaxSleepTime(
            TimeDuration.valueOf(100000, TimeUnit.MILLISECONDS))
        .build();
    return RaftClient.newBuilder()
        .setRaftGroup(mRaftGroup)
        .setClientId(mRawClientId)
        .setLeaderId(null)
        .setProperties(properties)
        .setParameters(parameters)
        .setRetryPolicy(retryPolicy)
        .build();
  }

  /**
   * @param reply from the ratis operation
   * @throws IOException
   */
  private void processReply(RaftClientReply reply, String msgToUser) throws IOException {
    if (!reply.isSuccess()) {
      IOException ioe = reply.getException() != null
          ? reply.getException()
          : new IOException(String.format("reply <%s> failed", reply));
      mPrintStream.printf("%s. Error: %s%n", msgToUser, ioe);
      throw new IOException(msgToUser);
    }
  }
}
