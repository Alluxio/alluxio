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
  // Start at 11000 to stay within the non-ephemeral port range and hopefully dodge most other
  // processes.
  private static final AtomicInteger NEXT_PORT = new AtomicInteger(11000);
  private static final Set<Integer> SKIP_PORTS = new HashSet(Arrays.asList(
      // add ports here to avoid conflicting with other processes on those ports.
  ));
  // for EmbeddedJournalIntegrationTestFaultTolerance
  public static final List<ReservedPort> EMBEDDED_JOURNAL_FAILOVER = allocate(3, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_SNAPSHOT_MASTER = allocate(3, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_SNAPSHOT_FOLLOWER = allocate(3, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_RESTART = allocate(3, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_RESTART_STRESS = allocate(3, 0);
  // for EmbeddedJournalIntegrationTestResizing
  public static final List<ReservedPort> EMBEDDED_JOURNAL_RESIZE = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_GROW = allocate(3, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_REPLACE_ALL = allocate(10, 0);
  // for EmbeddedJournalIntegrationTestTransferLeadership
  public static final List<ReservedPort> EMBEDDED_JOURNAL_TRANSFER_LEADER = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_REPEAT_TRANSFER_LEADER = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_RESET_PRIORITIES = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_ALREADY_TRANSFERRING = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_OUTSIDE_CLUSTER = allocate(5, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_NEW_MEMBER = allocate(6, 0);
  public static final List<ReservedPort> EMBEDDED_JOURNAL_UNAVAILABLE_MASTER = allocate(5, 0);

  public static final List<ReservedPort> JOURNAL_MIGRATION = allocate(3, 1);

  public static final List<ReservedPort> BACKUP_RESTORE_EMBEDDED = allocate(3, 1);

  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_WORKERS = allocate(1, 2);
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_NODES = allocate(2, 2);
  public static final List<ReservedPort> CONFIG_CHECKER_UNSET_VS_SET = allocate(2, 0);
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_MASTERS = allocate(2, 0);
  public static final List<ReservedPort> CONFIG_CHECKER_MULTI_MASTERS_EMBEDDED_HA = allocate(2, 0);

  public static final List<ReservedPort> MULTI_PROCESS_SIMPLE_CLUSTER = allocate(1, 1);
  public static final List<ReservedPort> MULTI_PROCESS_ZOOKEEPER = allocate(3, 2);

  public static final List<ReservedPort> JOURNAL_STOP_SINGLE_MASTER = allocate(1, 0);
  public static final List<ReservedPort> JOURNAL_STOP_MULTI_MASTER = allocate(3, 0);

  public static final List<ReservedPort> BACKUP_RESTORE_ZK = allocate(3, 1);
  public static final List<ReservedPort> BACKUP_RESTORE_SINGLE = allocate(1, 1);
  public static final List<ReservedPort> BACKUP_DELEGATION_PROTOCOL = allocate(3, 1);
  public static final List<ReservedPort> BACKUP_DELEGATION_FAILOVER_PROTOCOL = allocate(2, 1);
  public static final List<ReservedPort> BACKUP_DELEGATION_ZK = allocate(2, 1);
  public static final List<ReservedPort> BACKUP_DELEGATION_EMBEDDED = allocate(2, 1);
  public static final List<ReservedPort> BACKUP_RESTORE_METASSTORE_HEAP = allocate(1, 1);
  public static final List<ReservedPort> BACKUP_RESTORE_METASSTORE_ROCKS = allocate(1, 1);

  public static final List<ReservedPort> ZOOKEEPER_FAILURE = allocate(2, 1);
  public static final List<ReservedPort> ZOOKEEPER_CONNECTION_POLICY_STANDARD = allocate(2, 0);
  public static final List<ReservedPort> ZOOKEEPER_CONNECTION_POLICY_SESSION = allocate(2, 0);

  public static final List<ReservedPort> CHECKPOINT = allocate(2, 0);

  public static final List<ReservedPort> TRIGGERED_UFS_CHECKPOINT = allocate(1, 1);
  public static final List<ReservedPort> TRIGGERED_EMBEDDED_CHECKPOINT = allocate(1, 1);

  public static final List<ReservedPort> BACKWARDS_COMPATIBILITY = allocate(1, 1);

  public static final List<ReservedPort> MULTI_MASTER_URI = allocate(3, 1);
  public static final List<ReservedPort> ZOOKEEPER_URI = allocate(3, 2);

  public static final List<ReservedPort> QUORUM_SHELL = allocate(3, 0);
  public static final List<ReservedPort> QUORUM_SHELL_INFO = allocate(3, 0);
  public static final List<ReservedPort> QUORUM_SHELL_REMOVE = allocate(5, 0);

  private static synchronized List<ReservedPort> allocate(int numMasters, int numWorkers) {
    int needed = numMasters * MultiProcessCluster.PORTS_PER_MASTER
        + numWorkers * MultiProcessCluster.PORTS_PER_WORKER;
    Builder<ReservedPort> ports = ImmutableList.builder();
    for (int i = 0; i < needed; i++) {
      int port = NEXT_PORT.getAndIncrement();
      while (SKIP_PORTS.contains(port)) {
        port = NEXT_PORT.getAndIncrement();
      }
      ports.add(new ReservedPort(port));
    }
    return ports.build();
  }

  /**
   * A port that has been reserved for the purposes of a single test.
   */
  public static class ReservedPort {
    private final int mPort;

    private ReservedPort(int port) {
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
