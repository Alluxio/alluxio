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

package alluxio.hub.agent.util.process;

import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.ProcessState;
import alluxio.master.AlluxioJobMaster;
import alluxio.master.AlluxioMaster;
import alluxio.master.AlluxioSecondaryMaster;
import alluxio.proxy.AlluxioProxy;
import alluxio.worker.AlluxioJobWorker;
import alluxio.worker.AlluxioWorker;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A new instance of this class will reflect the current status of each type of the
 * Alluxio processes at creation time.
 */
public class NodeStatus {

  /**
   * Map process type to class name.
   */
  private static final Map<AlluxioNodeType, Class<?>> CLASS_MAPPING = new HashMap<>();

  static {
    CLASS_MAPPING.put(AlluxioNodeType.MASTER, AlluxioMaster.class);
    CLASS_MAPPING.put(AlluxioNodeType.SECONDARY_MASTER, AlluxioSecondaryMaster.class);
    CLASS_MAPPING.put(AlluxioNodeType.WORKER, AlluxioWorker.class);
    CLASS_MAPPING.put(AlluxioNodeType.JOB_MASTER, AlluxioJobMaster.class);
    CLASS_MAPPING.put(AlluxioNodeType.JOB_WORKER, AlluxioJobWorker.class);
    CLASS_MAPPING.put(AlluxioNodeType.PROXY, AlluxioProxy.class);
  }

  @Nullable
  static Class<?> lookupClass(AlluxioNodeType type) {
    return CLASS_MAPPING.get(type);
  }

  /**
   * Gets the PID of a running Alluxio process type. -1
   *
   * @param type type of Alluxio process to run
   * @param ps the process table to perform lookups for
   * @param javaPids a list of possible java pids, if null, re-reads the process table
   * @return the PID of the alluxio node type. -1 if there is no such PID found
   * @throws IOException if there is more than one PID detected with the class on the same machine
   */
  static Integer getPid(AlluxioNodeType type, ProcessTable ps, @Nullable List<Integer> javaPids)
      throws IOException {
    Class<?> main = lookupClass(type);
    if (main == null) {
      throw new IllegalArgumentException("Alluxio node type " + type
              + " has no main class mapping.");
    }

    List<Integer> pids = ps.getJavaPid(javaPids == null ? null : javaPids.stream(), main);
    if (pids.size() > 1) {
      throw new IOException("There were more than 1 process outputs which contained the "
          + "class of interest: " + main.getCanonicalName());
    }
    if (pids.size() == 0) {
      return -1;
    }
    return pids.get(0);
  }

  private final ProcessTable mPs;
  private final List<Integer> mJavaPids;

  /**
   * Create a new {@link NodeStatus} which queries the process table.
   *
   * @param ps the {@link LinuxProcessTable} used to get table information
   * @throws IOException if the pids fail to be retrieved
   */
  NodeStatus(ProcessTable ps) throws IOException {
    mPs = ps;
    mJavaPids = mPs.getJavaPids().collect(Collectors.toList());
  }

  /**
   * Create a new NodeStatus which queries the system's process table.
   *
   * Each instance of this class caches the pid entries at the time of creation.
   * Create a new instance if you want to check state over a period of time.
   *
   * @throws IOException if the process table can't be queried
   */
  public NodeStatus() throws IOException {
    this(ProcessTable.Factory.getInstance());
  }

  /**
   * Get the current process status.
   *
   * @param type the process type to get information on
   * @return status of the process
   * @throws IOException if the information for this process can't be retrieved
   */
  public AlluxioProcessStatus getProcessStatus(AlluxioNodeType type) throws IOException {
    int pid = getPid(type, mPs, mJavaPids);
    AlluxioProcessStatus.Builder builder = AlluxioProcessStatus.newBuilder()
        .setNodeType(type);
    if (pid > 0) {
      builder.setState(ProcessState.RUNNING);
      builder.setPid(pid);
    } else {
      builder.setState(ProcessState.STOPPED);
    }
    return builder.build();
  }
}
