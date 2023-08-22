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

package alluxio.cli.fsadmin.report;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.meta.MetaMasterClient;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterInfoField;
import alluxio.grpc.MasterVersion;
import alluxio.grpc.NetAddress;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Prints Alluxio cluster summarized information.
 */
public class SummaryCommand {
  private final MetaMasterClient mMetaMasterClient;
  private final BlockMasterClient mBlockMasterClient;
  private final PrintStream mPrintStream;
  private final String mDateFormatPattern;

  /**
   * Creates a new instance of {@link SummaryCommand}.
   *
   * @param metaMasterClient client to connect to meta master
   * @param blockMasterClient client to connect to block master
   * @param dateFormatPattern the pattern to follow when printing the date
   * @param printStream stream to print summary information to
   */
  public SummaryCommand(MetaMasterClient metaMasterClient,
      BlockMasterClient blockMasterClient, String dateFormatPattern, PrintStream printStream) {
    mMetaMasterClient = metaMasterClient;
    mBlockMasterClient = blockMasterClient;
    mPrintStream = printStream;
    mDateFormatPattern = dateFormatPattern;
  }

  /**
   * Runs report summary command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode summeryInfo = mapper.createObjectNode();

    Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
            .asList(MasterInfoField.LEADER_MASTER_ADDRESS, MasterInfoField.WEB_PORT,
                    MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
                    MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
                    MasterInfoField.SAFE_MODE, MasterInfoField.ZOOKEEPER_ADDRESSES,
                    MasterInfoField.RAFT_JOURNAL, MasterInfoField.RAFT_ADDRESSES,
                    MasterInfoField.MASTER_VERSION));
    MasterInfo masterInfo = mMetaMasterClient.getMasterInfo(masterInfoFilter);

    summeryInfo.put("Master Address", masterInfo.getLeaderMasterAddress());
    summeryInfo.put("Web Port", masterInfo.getWebPort());
    summeryInfo.put("Rpc Port", masterInfo.getRpcPort());
    summeryInfo.put("Started", CommonUtils.convertMsToDate(masterInfo.getStartTimeMs(), mDateFormatPattern));
    summeryInfo.put("Uptime", CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()));
    summeryInfo.put("Version", masterInfo.getVersion());
    summeryInfo.put("Safe Mode", masterInfo.getSafeMode());

    ObjectNode zooKeeper = mapper.createObjectNode();
    List<String> zookeeperAddresses = masterInfo.getZookeeperAddressesList();
    ArrayNode zkAddresses = mapper.createArrayNode();
    if (zookeeperAddresses == null || zookeeperAddresses.isEmpty()) {
      zooKeeper.put("Enabled", "false");
    } else {
      zooKeeper.put("Enabled", "true");
      for (String zookeeperAddress : zookeeperAddresses) {
        zkAddresses.add(zookeeperAddress);
      }
    }
    zooKeeper.set("Addresses", zkAddresses);
    summeryInfo.set("Zookeeper", zooKeeper);

    ObjectNode raftJournal = mapper.createObjectNode();
    ArrayNode raftAddresses = mapper.createArrayNode();
    if (!masterInfo.getRaftJournal()) {
      raftJournal.put("Enabled", "false");
    } else {
      raftJournal.put("Enabled", "true");
      for (String raftAddress : masterInfo.getRaftAddressList()) {
        raftAddresses.add(raftAddress);
      }
    }
    raftJournal.set("Addresses", raftAddresses);
    summeryInfo.set("Raft Journal", raftJournal);

    ArrayNode mVersions = mapper.createArrayNode();
    for (MasterVersion masterVersion: masterInfo.getMasterVersionsList()) {
      NetAddress address = masterVersion.getAddresses();
      ObjectNode mVersion = mapper.createObjectNode();
      mVersion.put("Host", masterVersion.getAddresses().getHost());
      mVersion.put("Port", masterVersion.getAddresses().getRpcPort());
      mVersion.put("State", masterVersion.getState());
      mVersion.put("Version", masterVersion.getVersion());
      mVersions.add(mVersion);
    }
    summeryInfo.set("Master status", mVersions);

    Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
            .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
                    BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.USED_BYTES,
                    BlockMasterInfoField.FREE_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
                    BlockMasterInfoField.USED_BYTES_ON_TIERS));
    BlockMasterInfo blockMasterInfo = mBlockMasterClient.getBlockMasterInfo(blockMasterInfoFilter);

    summeryInfo.put("Live Workers", blockMasterInfo.getLiveWorkerNum());
    summeryInfo.put("Lost Workers", blockMasterInfo.getLostWorkerNum());

    ArrayNode totalCapacity = mapper.createArrayNode();
    ObjectNode allTotalCapacity = mapper.createObjectNode();
    allTotalCapacity.put("Tier", "ALL");
    allTotalCapacity.put("Size", FormatUtils.getSizeFromBytes(blockMasterInfo.getCapacityBytes()));
    totalCapacity.add(allTotalCapacity);
    Map<String, Long> totalCapacityOnTiers = new TreeMap<>((a, b)
            -> (FileSystemAdminShellUtils.compareTierNames(a, b)));
    totalCapacityOnTiers.putAll(blockMasterInfo.getCapacityBytesOnTiers());
    for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
      ObjectNode tierTotalCapacity = mapper.createObjectNode();
      tierTotalCapacity.put("Tier", capacityBytesTier.getKey());
      tierTotalCapacity.put("Size", FormatUtils.getSizeFromBytes(capacityBytesTier.getValue()));
      totalCapacity.add(tierTotalCapacity);
    }
    summeryInfo.set("Total Capacity", totalCapacity);

    ArrayNode usedCapacity = mapper.createArrayNode();
    ObjectNode allUsedCapacity = mapper.createObjectNode();
    allUsedCapacity.put("Tier", "ALL");
    allUsedCapacity.put("Size", FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()));
    usedCapacity.add(allUsedCapacity);
    Map<String, Long> usedCapacityOnTiers = new TreeMap<>((a, b)
            -> (FileSystemAdminShellUtils.compareTierNames(a, b)));
    usedCapacityOnTiers.putAll(blockMasterInfo.getUsedBytesOnTiers());
    for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
      ObjectNode tierUsedCapacity = mapper.createObjectNode();
      tierUsedCapacity.put("Tier", usedBytesTier.getKey());
      tierUsedCapacity.put("Size", FormatUtils.getSizeFromBytes(usedBytesTier.getValue()));
      usedCapacity.add(tierUsedCapacity);
    }
    summeryInfo.set("Used Capacity", usedCapacity);

    summeryInfo.put("Free Capacity", FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes()));

    mPrintStream.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(summeryInfo));
    return 0;
  }
}
