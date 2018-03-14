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
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterClientConfig;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;
import alluxio.wire.BlockMasterInfo.BlockMasterInfoField;
import alluxio.wire.MasterInfo;
import alluxio.wire.MasterInfo.MasterInfoField;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Print Alluxio cluster summarized information.
 * This class depends on meta master client and block master client.
 */
public class SummaryCommand {
  /** Print Alluxio cluster summarized information. */
  public static void printSummary() {
    System.out.println("Alluxio Cluster Summary: ");

    int numOfSpaces = 4;
    String spaces = String.format("%" + numOfSpaces + "s", "");

    printMetaMasterInfo(spaces);
    printBlockMasterInfo(spaces);
  }

  /**
   * Prints the meta master info.
   *
   * @param spaces for better print out message indentations
   */
  private static void printMetaMasterInfo(String spaces) {
    try (MetaMasterClient client =
             new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      Set<MasterInfoField> masterInfoFilter = new HashSet<>(Arrays
          .asList(MasterInfoField.MASTER_ADDRESS, MasterInfoField.WEB_PORT,
              MasterInfoField.RPC_PORT, MasterInfoField.START_TIME_MS,
              MasterInfoField.UP_TIME_MS, MasterInfoField.VERSION,
              MasterInfoField.SAFE_MODE));
      MasterInfo masterInfo = client.getMasterInfo(masterInfoFilter);

      System.out.println(spaces + "Master Address: " + masterInfo.getMasterAddress());
      System.out.println(spaces + "Web Port: " + masterInfo.getWebPort());
      System.out.println(spaces + "Rpc Port: " + masterInfo.getRpcPort());
      System.out.println(spaces + "Started: "
          + CommonUtils.convertMsToDate(masterInfo.getStartTimeMs()));
      System.out.println(spaces + "Uptime: "
          + CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()));
      System.out.println(spaces + "Version: " + masterInfo.getVersion());
      System.out.println(spaces + "Safe Mode: " + masterInfo.isSafeMode());
    } catch (UnavailableException e) {
      e.printStackTrace();
      System.out.println("Please check the Alluxio master status.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Prints the block master info.
   *
   * @param spaces for better print out message indentations
   */
  private static void printBlockMasterInfo(String spaces) {
    try (RetryHandlingBlockMasterClient client =
             new RetryHandlingBlockMasterClient(MasterClientConfig.defaults())) {
      Set<BlockMasterInfoField> blockMasterInfoFilter = new HashSet<>(Arrays
          .asList(BlockMasterInfoField.LIVE_WORKER_NUM, BlockMasterInfoField.LOST_WORKER_NUM,
              BlockMasterInfoField.CAPACITY_BYTES, BlockMasterInfoField.USED_BYTES,
              BlockMasterInfoField.FREE_BYTES, BlockMasterInfoField.CAPACITY_BYTES_ON_TIERS,
              BlockMasterInfoField.USED_BYTES_ON_TIERS));
      BlockMasterInfo blockMasterInfo = client.getBlockMasterInfo(blockMasterInfoFilter);

      System.out.println(spaces + "Live workers: " + blockMasterInfo.getLiveWorkerNum());
      System.out.println(spaces + "Lost workers: " + blockMasterInfo.getLostWorkerNum());

      System.out.println(spaces + "Total Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getCapacityBytes()));
      Map<String, Long> totalCapacityOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
      for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
        long value = capacityBytesTier.getValue();
        if (value > 0) {
          System.out.println(spaces + spaces + "Tier: " + capacityBytesTier.getKey()
              + "  Size: " + FormatUtils.getSizeFromBytes(value));
        }
      }

      System.out.println(spaces + "Used Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()));
      Map<String, Long> usedCapacityOnTiers = blockMasterInfo.getUsedBytesOnTiers();
      for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
        long value = usedBytesTier.getValue();
        if (value > 0) {
          System.out.println(spaces + spaces + "Tier: " + usedBytesTier.getKey()
              + "  Size: " + FormatUtils.getSizeFromBytes(value));
        }
      }

      System.out.println(spaces + "Free Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
