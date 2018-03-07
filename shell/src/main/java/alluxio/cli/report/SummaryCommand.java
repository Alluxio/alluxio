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

package alluxio.cli.report;

import alluxio.client.MetaMasterClient;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.master.MasterClientConfig;
import alluxio.wire.ClusterInfo;

/**
 * Print Alluxio cluster summarized information.
 */
public class SummaryCommand {

  /** Print Alluxio cluster summarized information.*/
  public static void printSummary() {
    System.out.println("Alluxio Cluster Summary: ");
    try (MetaMasterClient client =
            new RetryHandlingMetaMasterClient(MasterClientConfig.defaults())) {
      ClusterInfo info = client.getClusterInfo();
      System.out.println("    Master Address: " + info.getMasterAddress());
      System.out.println("    Started: " + info.getStartTime());
      System.out.println("    Uptime: " + info.getUpTime());
      System.out.println("    Version: " + info.getVersion());
      System.out.println("    Safe Mode: " + info.isSafeMode());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
