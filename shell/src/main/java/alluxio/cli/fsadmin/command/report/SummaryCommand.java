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
import alluxio.wire.ClusterInfo;

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
      ClusterInfo clusterInfo = client.getClusterInfo();
      System.out.println("    Master Address: " + clusterInfo.getMasterAddress());
      System.out.println("    Web Port: " + clusterInfo.getWebPort());
      System.out.println("    Rpc Port: " + clusterInfo.getRpcPort());
      System.out.println("    Started: " + clusterInfo.getStartTime());
      System.out.println("    Uptime: " + clusterInfo.getUpTime());
      System.out.println("    Version: " + clusterInfo.getVersion());
      System.out.println("    High Availability Mode: " + clusterInfo.isHAMode());
      System.out.println("    Safe Mode: " + clusterInfo.isSafeMode());
    } catch (Exception e) {
      e.printStackTrace();
    }

    try (RetryHandlingBlockMasterClient client =
        new RetryHandlingBlockMasterClient(MasterClientConfig.defaults())) {
      BlockMasterInfo blockMasterInfo = client.getBlockMasterInfo();
      System.out.println("    Live workers: " + blockMasterInfo.getLiveWorkerNum());
      System.out.println("    Dead workers: " + blockMasterInfo.getDeadWorkerNum());
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
