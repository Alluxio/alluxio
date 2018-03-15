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

import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.MetaMasterClient;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import com.google.common.base.Strings;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Print Alluxio cluster summarized information.
 * This class depends on meta master client and block master client.
 */
public class SummaryCommand {
  private static final int INDENT_SIZE = 4;
  private MetaMasterClient mMetaMasterClient;
  private RetryHandlingBlockMasterClient mBlockMasterClient;

  private int mIndentationLevel = 1;

  /**
   * Prints summarized Alluxio cluster information.
   *
   * @param metaMasterClient client to connect to meta master
   * @param blockMasterClient client to connect to block master
   */
  public SummaryCommand(MetaMasterClient metaMasterClient,
      RetryHandlingBlockMasterClient blockMasterClient) {
    mMetaMasterClient = metaMasterClient;
    mBlockMasterClient = blockMasterClient;
  }

  /**
   * Runs report summary command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run() throws IOException {
    System.out.println("Alluxio Cluster Summary: ");
    printMetaMasterInfo();
    printBlockMasterInfo();
    return 0;
  }

  /**
   * Prints Alluxio meta master information.
   */
  private void printMetaMasterInfo() throws IOException {
    Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
        .asList(MasterInfoField.MASTER_ADDRESS, MasterInfoField.WEB_PORT,
            MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
            MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
            MasterInfoField.SAFE_MODE));
    MasterInfo masterInfo = mMetaMasterClient.getMasterInfo(masterInfoFilter);

    print("Master Address: " + masterInfo.getMasterAddress());
    print("Web Port: " + masterInfo.getWebPort());
    print("Rpc Port: " + masterInfo.getRpcPort());
    print("Started: " + CommonUtils.convertMsToDate(masterInfo.getStartTimeMs()));
    print("Uptime: " + CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()));
    print("Version: " + masterInfo.getVersion());
    print("Safe Mode: " + masterInfo.isSafeMode());
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

    Map<String, Long> totalCapacityOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
    if (totalCapacityOnTiers != null) {
      mIndentationLevel = 2;
      for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
        long value = capacityBytesTier.getValue();
        print("Tier: " + capacityBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(value));
      }
    }

    mIndentationLevel = 1;
    print("Used Capacity: "
        + FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()));

    Map<String, Long> usedCapacityOnTiers = blockMasterInfo.getUsedBytesOnTiers();
    if (usedCapacityOnTiers != null) {
      mIndentationLevel = 2;
      for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
        long value = usedBytesTier.getValue();
        print("Tier: " + usedBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(value));
      }
    }

    mIndentationLevel = 1;
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
    System.out.println(indent + text);
  }
}
