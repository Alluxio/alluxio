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

package alluxio.cli.fsadmin.command.report;

import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.master.MasterClientConfig;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import com.google.common.base.Strings;
import org.apache.commons.cli.CommandLine;

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

  public int run(CommandLine cl) throws IOException {
    System.out.println("Alluxio Cluster Summary: ");

    printMetaMasterInfo();
    printBlockMasterInfo();
    return 0;
  }

  /**
   * Prints Alluxio meta master information.
   */
  private void printMetaMasterInfo() throws IOException{
    try (MetaMasterClient client =
             new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
          .asList(MasterInfoField.MASTER_ADDRESS, MasterInfoField.WEB_PORT,
              MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
              MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
              MasterInfoField.SAFE_MODE));
      MasterInfo masterInfo = client.getMasterInfo(masterInfoFilter);
      if (masterInfo == null) {
        throw new IOException("Cannot get meta master info from meta master client");
      }
      print("Master Address: " + masterInfo.getMasterAddress(), 1);
      print("Web Port: " + masterInfo.getWebPort(), 1);
      print("Rpc Port: " + masterInfo.getRpcPort(), 1);
      print("Started: " + CommonUtils.convertMsToDate(masterInfo.getStartTimeMs()), 1);
      print("Uptime: " + CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()), 1);
      print("Version: " + masterInfo.getVersion(), 1);
      print("Safe Mode: " + masterInfo.isSafeMode(), 1);
    }
  }

  /**
   * Prints Alluxio block master information.
   */
  private void printBlockMasterInfo() throws IOException{
    try (RetryHandlingBlockMasterClient client =
             new RetryHandlingBlockMasterClient(MasterClientConfig.defaults())) {
      Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
          .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
              BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.USED_BYTES,
              BlockMasterInfoField.FREE_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
              BlockMasterInfoField.USED_BYTES_ON_TIERS));
      BlockMasterInfo blockMasterInfo = client.getBlockMasterInfo(blockMasterInfoFilter);
      if (blockMasterInfo == null) {
        throw new IOException("Cannot get block master info from block master client");
      }

      print("Live Workers: " + blockMasterInfo.getLiveWorkerNum(), 1);
      print("Lost Workers: " + blockMasterInfo.getLostWorkerNum(), 1);

      print("Total Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getCapacityBytes()), 1);

      Map<String, Long> totalCapacityOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
      if (totalCapacityOnTiers != null) {
        for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
          long value = capacityBytesTier.getValue();
          print("Tier: " + capacityBytesTier.getKey()
              + "  Size: " + FormatUtils.getSizeFromBytes(value), 2);
        }
      }

      print("Used Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()), 1);

      Map<String, Long> usedCapacityOnTiers = blockMasterInfo.getUsedBytesOnTiers();
      if (usedCapacityOnTiers != null) {
        for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
          long value = usedBytesTier.getValue();
          print("Tier: " + usedBytesTier.getKey()
              + "  Size: " + FormatUtils.getSizeFromBytes(value), 2);
        }
      }

      print("Free Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes()), 1);
    }
  }

  /**
   * Prints indented information.
   *
   * @param text information to print
   * @param indentLevel indentation level to use
   */
  private void print(String text, int indentLevel) {
    String indent = Strings.repeat(" ", indentLevel * INDENT_SIZE);
    System.out.println(indent + text);
  }
}
