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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

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
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_WORKERS = allocate(1, 2);
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_NODES = allocate(2, 2);
  public static final List<ReservedPort> CONFIG_CHECKER_UNSET_VS_SET = allocate(2, 0);
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_MASTERS = allocate(2, 0);

  public static final List<ReservedPort> MULTI_PROCESS_SIMPLE_CLUSTER = allocate(1, 1);
  public static final List<ReservedPort> MULTI_PROCESS_ZOOKEEPER = allocate(3, 2);

  public static final List<ReservedPort> JOURNAL_STOP_SINGLE_MASTER = allocate(1, 0);
  public static final List<ReservedPort> JOURNAL_STOP_MULTI_MASTER = allocate(3, 0);

  public static final List<ReservedPort> BACKUP_RESTORE_ZK = allocate(3, 1);
  public static final List<ReservedPort> BACKUP_RESTORE_SINGLE = allocate(1, 1);

  public static final List<ReservedPort> ZOOKEEPER_FAILURE = allocate(1, 1);

  // Start at 11000 to stay within the non-ephemeral port range and hopefully dodge most other
  // processes.
  private static final AtomicInteger NEXT_PORT = new AtomicInteger(11000);

  private static final Set<Integer> SKIP_PORTS = new HashSet(Arrays.asList(
      // add ports here to avoid conflicting with other processes on those ports.
  ));

  private static synchronized List<ReservedPort> allocate(int numMasters, int numWorkers) {
    int needed = 2 * numMasters + 3 * numWorkers;
    Builder<ReservedPort> ports = ImmutableList.builder();
    for (int i = 0; i < needed; i++) {
      ports.add(new ReservedPort());
    }
    return ports.build();
  }

  /**
   * A port that has been reserved for the purposes of a single test.
   */
  public static class ReservedPort {
    private int mPort;

    private ReservedPort() {
      int port = NEXT_PORT.getAndIncrement();
      while (SKIP_PORTS.contains(port)) {
        port = NEXT_PORT.getAndIncrement();
      }
      mPort = port;
    }

    /**
     * @return the port number
     */
    public int getPort() {
      return mPort;
    }
  }
}
