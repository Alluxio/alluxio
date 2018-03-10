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
      System.out.println("    Started: " + masterInfo.getStartTime());
      System.out.println("    Uptime: " + masterInfo.getUpTime());
      System.out.println("    Version: " + masterInfo.getVersion());
      System.out.println("    Safe Mode: " + masterInfo.isSafeMode());
    } catch (Exception e) {
      e.printStackTrace();
    }

    try (RetryHandlingBlockMasterClient client =
        new RetryHandlingBlockMasterClient(MasterClientConfig.defaults())) {
      BlockMasterInfo blockMasterInfo = client.getBlockMasterInfo();
      System.out.println("    Live workers: " + blockMasterInfo.getLiveWorkerNum());
      System.out.println("    Dead workers: " + blockMasterInfo.getLostWorkerNum());

      System.out.println("    Total Capacity: " + blockMasterInfo.getTotalCapacity());
      Map<String, String> totalCapacityOnTiers = blockMasterInfo.getTotalCapacityOnTiers();
      for (Map.Entry<String, String> capacityTier : totalCapacityOnTiers.entrySet()) {
        System.out.println("        Tier: " + capacityTier.getKey()
            + "  Size: " + capacityTier.getValue());
      }

      System.out.println("    Used Capacity: " + blockMasterInfo.getUsedCapacity());
      Map<String, String> usedCapacityOnTiers = blockMasterInfo.getUsedCapacityOnTiers();
      for (Map.Entry<String, String> usedCapacityTier : usedCapacityOnTiers.entrySet()) {
        System.out.println("        Tier: " + usedCapacityTier.getKey()
            + "  Size: " + usedCapacityTier.getValue());
      }

      System.out.println("    Free Capacity: " + blockMasterInfo.getFreeCapacity());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
