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

package alluxio.multi.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class for coordinating between test suites so that they don't conflict in ports.
 *
 * Using the same ports every time improves build stability when using Docker.
 */
public class PortCoordination {
  public static List<ReservedPort> sConfigCheckerMultiWorkersTest = allocate(1, 2);
  public static List<ReservedPort> sConfigCheckerMultiNodesTest = allocate(2, 2);
  public static List<ReservedPort> sConfigCheckerUnsetVsSet = allocate(2, 0);
  public static List<ReservedPort> sConfigCheckerMultiMastersTest = allocate(2, 0);

  public static List<ReservedPort> sMultiProcessSimpleCluster = allocate(1, 1);
  public static List<ReservedPort> sMultiProcessZookeeper = allocate(3, 2);

  public static List<ReservedPort> sSingleMasterJournalStopIntegration = allocate(1, 0);
  public static List<ReservedPort> sMultiMasterJournalStopIntegration = allocate(3, 0);

  public static List<ReservedPort> sBackupRestoreZk = allocate(3, 1);
  public static List<ReservedPort> sBackupRestoreSingle = allocate(1, 1);

  public static List<ReservedPort> sZookeeperFailure = allocate(1, 1);

  // Start at 11000 to stay within the non-ephemeral port range and hopefully dodge most other
  // processes.
  private static AtomicInteger sNextPort = new AtomicInteger(11000);

  private static final Set<Integer> SKIP_PORTS = new HashSet(Arrays.asList(
      // add ports here to avoid conflicting with other processes on those ports.
  ));

  private static synchronized List<ReservedPort> allocate(int numMasters, int numWorkers) {
    int needed = 2 * numMasters + 3 * numWorkers;
    List<ReservedPort> ports = new ArrayList();
    for (int i = 0; i < needed; i++) {
      ports.add(new ReservedPort());
    }
    return ports;
  }

  // This is intended for usage in a Docker environment where there won't be random processes using
  // these ports. If we find a process using one of these ports, we can remove the port from here
  // and replace it with a new one.
  public static class ReservedPort {
    private int mPort;

    private ReservedPort() {
      int port = sNextPort.getAndIncrement();
      while (SKIP_PORTS.contains(port)) {
        port = sNextPort.getAndIncrement();
      }
      mPort = port;
    }

    public int getPort() {
      return mPort;
    }
  }
}
