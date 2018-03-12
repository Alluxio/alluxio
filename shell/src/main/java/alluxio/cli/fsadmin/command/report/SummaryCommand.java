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
import alluxio.wire.MasterInfo;

import java.util.Map;

/**
 * Print Alluxio cluster summarized information.
 */
public class SummaryCommand {
  /** Print Alluxio cluster summarized information.*/
  public static void printSummary() {
    System.out.println("Alluxio Cluster Summary: ");

    try (MetaMasterClient client =
        new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      MasterInfo masterInfo = client.getMasterInfo();
      System.out.println("    Master Address: " + masterInfo.getMasterAddress());

      int webPort = masterInfo.getWebPort();
      if (webPort != 0) { // Alluxio web services are running
        System.out.println("    Web Port: " + webPort);
      }

      System.out.println("    Rpc Port: " + masterInfo.getRpcPort());
      System.out.println("    Started: "
          + CommonUtils.convertMsToDate(masterInfo.getStartTimeMs()));
      System.out.println("    Uptime: "
          + CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs()));
      System.out.println("    Version: " + masterInfo.getVersion());
      System.out.println("    Safe Mode: " + masterInfo.isSafeMode());
    } catch (Exception e) {
      e.printStackTrace();
    }

    try (RetryHandlingBlockMasterClient client =
        new RetryHandlingBlockMasterClient(MasterClientConfig.defaults())) {
      BlockMasterInfo blockMasterInfo = client.getBlockMasterInfo();
      System.out.println("    Live workers: " + blockMasterInfo.getLiveWorkerNum());
      System.out.println("    Lost workers: " + blockMasterInfo.getLostWorkerNum());

      System.out.println("    Total Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getCapacityBytes()));
      Map<String, Long> totalCapacityOnTiers = blockMasterInfo.getCapacityBytesOnTiers();
      for (Map.Entry<String, Long> capacityBytesTier : totalCapacityOnTiers.entrySet()) {
        System.out.println("        Tier: " + capacityBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(capacityBytesTier.getValue()));
      }

      System.out.println("    Used Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getUsedBytes()));
      Map<String, Long> usedCapacityOnTiers = blockMasterInfo.getUsedBytesOnTiers();
      for (Map.Entry<String, Long> usedBytesTier: usedCapacityOnTiers.entrySet()) {
        System.out.println("        Tier: " + usedBytesTier.getKey()
            + "  Size: " + FormatUtils.getSizeFromBytes(usedBytesTier.getValue()));
      }

      System.out.println("    Free Capacity: "
          + FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
