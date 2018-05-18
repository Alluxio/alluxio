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

package alluxio.cli.fsadmin;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.exception.status.UnavailableException;
import alluxio.master.MasterInquireClient;
import alluxio.master.PollingMasterInquireClient;
import alluxio.resource.CloseableResource;
import alluxio.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Class for convenience methods used by {@link FileSystemAdminShell}.
 */
public final class FileSystemAdminShellUtils {

  private FileSystemAdminShellUtils() {} // prevent instantiation

  /**
   * Compares two tier names according to their rank values.
   *
   * @param a one tier name
   * @param b another tier name
   * @return compared result
   */
  public static int compareTierNames(String a, String b) {
    int aValue = getTierRankValue(a);
    int bValue = getTierRankValue(b);
    if (aValue == bValue) {
      return a.compareTo(b);
    }
    return aValue - bValue;
  }

  /**
   * Checks if the master client service is running.
   * Throws an exception if fails to determine that the master client service is running.
   */
  public static void masterClientServiceIsRunning() throws IOException {
    // Check if Alluxio master and client services are running
    try (CloseableResource<FileSystemMasterClient> client =
             FileSystemContext.INSTANCE.acquireMasterClientResource()) {
      MasterInquireClient inquireClient = null;
      try {
        InetSocketAddress address = client.get().getAddress();
        List<InetSocketAddress> addresses = Arrays.asList(address);
        inquireClient = new PollingMasterInquireClient(addresses, () ->
            new ExponentialBackoffRetry(50, 100, 2));
      } catch (UnavailableException e) {
        throw new IOException("Failed to get the leader master, "
            + "please check your Alluxio master status.");
      }
      try {
        inquireClient.getPrimaryRpcAddress();
      } catch (UnavailableException e) {
        throw new IOException("The Alluxio leader master is not currently serving requests, "
            + "please check your Alluxio master status.");
      }
    }
  }

  /**
   * Assigns a rank value to the input string.
   *
   * @param input the input to turn to rank value
   * @return a rank value used to sort tiers
   */
  private static int getTierRankValue(String input) {
    // MEM, SSD, and HDD are the most commonly used Alluxio tier alias,
    // so we want them to show before other tier names
    // MEM, SSD, and HDD are sorted according to the speed of access
    List<String> tierOrder = Arrays.asList("MEM", "SSD", "HDD");
    int rank = tierOrder.indexOf(input);
    if (rank == -1) {
      return Integer.MAX_VALUE;
    }
    return rank;
  }
}
