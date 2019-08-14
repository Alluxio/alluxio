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
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;

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
  private static final int INDENT_SIZE = 4;

  private int mIndentationLevel = 0;
  private MetaMasterClient mMetaMasterClient;
  private BlockMasterClient mBlockMasterClient;
  private PrintStream mPrintStream;
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
    print("Alluxio cluster summary: ");
    printMetaMasterInfo();
    printBlockMasterInfo();
    return 0;
  }

  /**
   * Prints Alluxio meta master information.
   */
  private void printMetaMasterInfo() throws IOException {
    mIndentationLevel++;
    Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
        .asList(MasterInfoField.LEADER_MASTER_ADDRESS, MasterInfoField.WEB_PORT,
            MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
            MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
            MasterInfoField.SAFE_MODE, MasterInfoField.ZOOKEEPER_ADDRESSES));
    MasterInfo masterInfo = mMetaMasterClient.getMasterInfo(masterInfoFilter);

    print("Master Address: " + masterInfo.getLeaderMasterAddress());
    print("Web Port: " + masterInfo.getWebPort());
    print("Rpc Port: " + masterInfo.getRpcPort());
    print("Started: " + CommonUtils.convertMsToDate(masterInfo.getStartTimeMs(),
        mDateFormatPattern));
    print("Uptime: " + CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()));
    print("Version: " + masterInfo.getVersion());
    print("Safe Mode: " + masterInfo.getSafeMode());

    List<String> zookeeperAddresses = masterInfo.getZookeeperAddressesList();
    if (zookeeperAddresses == null || zookeeperAddresses.isEmpty()) {
      print("Zookeeper Enabled: false");
    } else {
      print("Zookeeper Enabled: true");
      print("Zookeeper Addresses: ");
      mIndentationLevel++;
      for (String zkAddress : zookeeperAddresses) {
        print(zkAddress);
      }
      mIndentationLevel--;
    }
  }

  /**
   * Prints Alluxio block master information.
   */
  private void printBlockMasterInfo() throws IOException {
    Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
        .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
            BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.USED_BYTES,
            BlockMasterInfoField.FREE_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
            BlockMasterInfoField.USED_BYTES_ON_TIERS));
    BlockMasterInfo blockMasterInfo = mBlockMasterClient.getBlockMasterInfo(blockMasterInfoFilter);

    print("Live Workers: " + blockMasterInfo.getLiveWorkerNum());
    print("Lost Workers: " + blockMasterInfo.getLostWorkerNum());

    print("Total Capacity: "
        + FormatUtils.getSizeFromBytes(blockMasterInfo.getCapacityBytes()));

    mIndentationLevel++;
    Map<String, Long> totalCapacityOnTiers = new TreeMap<>((a, b)
        -> (FileSystemAdminShellUtils.compareTierNames(a, b)));
    totalCapacityOnTiers.putAll(blockMasterInfo.getCapacityBytesOnTiers());
    for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
      print("Tier: " + capacityBytesTier.getKey()
          + "  Size: " + FormatUtils.getSizeFromBytes(capacityBytesTier.getValue()));
    }

    mIndentationLevel--;
    print("Used Capacity: "
        + FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()));

    mIndentationLevel++;
    Map<String, Long> usedCapacityOnTiers = new TreeMap<>((a, b)
        -> (FileSystemAdminShellUtils.compareTierNames(a, b)));
    usedCapacityOnTiers.putAll(blockMasterInfo.getUsedBytesOnTiers());
    for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
      print("Tier: " + usedBytesTier.getKey()
          + "  Size: " + FormatUtils.getSizeFromBytes(usedBytesTier.getValue()));
    }

    mIndentationLevel--;
    print("Free Capacity: "
        + FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes()));
  }

  /**
   * Prints indented information.
   *
   * @param text information to print
   */
  private void print(String text) {
    String indent = Strings.repeat(" ", mIndentationLevel * INDENT_SIZE);
    mPrintStream.println(indent + text);
  }
}
