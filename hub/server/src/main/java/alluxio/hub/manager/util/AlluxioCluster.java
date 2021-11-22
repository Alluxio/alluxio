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

package alluxio.hub.manager.util;

import alluxio.hub.manager.process.ManagerProcessContext;
import alluxio.hub.proto.AlluxioNodeStatus;
import alluxio.hub.proto.AlluxioNodeStatusOrBuilder;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.AlluxioProcessStatus;
import alluxio.hub.proto.ProcessState;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * A class representing the state of a running Alluxio cluster.
 */
public class AlluxioCluster implements ProtoConverter<alluxio.hub.proto.AlluxioCluster> {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCluster.class);

  /**
   * Maps node hostname to a map of node type and process state.
   */
  private final EventListeningConcurrentHashMap<String, Map<AlluxioNodeType, ProcessState>>
      mCluster;
  private ManagerProcessContext mCtx;

  /**
   * Creates a new instance of {@link AlluxioCluster}.
   *
   * @param svc the executor service used to run event handlers
   */
  public AlluxioCluster(ExecutorService svc) {
    mCluster = new EventListeningConcurrentHashMap<>(svc);
  }

  /**
   * Sets ManagerProcessContext and registers an event to send heartbeats.
   * @param ctx Manager process context
   */
  public void setContext(ManagerProcessContext ctx) {
    if (mCtx == null) {
      mCtx = ctx;
      mCluster.registerEventListener(() -> {
        mCtx.alluxioClusterHeartbeat(toProto());
      }, 1000);
    }
  }

  /**
   * Add a new node to the cluster.
   *
   * @param addr the alluxio node address to add
   * @return true if this set did not already include this process
   */
  public Map<AlluxioNodeType, ProcessState> heartbeat(final AlluxioNodeStatusOrBuilder addr) {
    Preconditions.checkArgument(addr.hasHostname(), "address must include hostname");
    Map<AlluxioNodeType, ProcessState> map = addr.getProcessList().stream()
        .peek(proc -> {
          Preconditions.checkArgument(proc.hasNodeType());
          Preconditions.checkArgument(proc.hasState());
        }).collect(
            Collectors.toMap(AlluxioProcessStatus::getNodeType, AlluxioProcessStatus::getState));
    return mCluster.put(addr.getHostname(), map);
  }

  /**
   * Remove a node from the cluster with the address's hostname.
   *
   * @param addr the node of the Alluxio cluster to remove statuses for
   * @return the most recent status of the Alluxio processes known on that node
   */
  public Map<AlluxioNodeType, ProcessState> remove(final AlluxioNodeStatusOrBuilder addr) {
    return remove(addr.getHostname());
  }

  /**
   * Remove a node from the cluster.
   *
   * @param hostname the node to remove with the given hostname
   * @return the most recent status of the Alluxio processes known on that node
   */
  public Map<AlluxioNodeType, ProcessState> remove(final String hostname) {
    return mCluster.remove(hostname);
  }

  /**
   * Gets a set of node hostnames which correspond to a given {@link AlluxioNodeType}.
   *
   * @param type the type to query for
   * @return a set of hosts containing running the desired node
   */
  public Set<String> getNodeType(AlluxioNodeType type) {
    return mCluster.entrySet().stream()
        .filter(e -> e.getValue().containsKey(type))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * @return the total number of nodes in the cluster (masters + workers)
   */
  public int size() {
    return mCluster.size();
  }

  @Override
  public alluxio.hub.proto.AlluxioCluster toProto() {
    alluxio.hub.proto.AlluxioCluster.Builder builder =
        alluxio.hub.proto.AlluxioCluster.newBuilder();
    mCluster.forEach((hostname, procs) -> {
      builder.addNode(AlluxioNodeStatus.newBuilder()
          .setHostname(hostname)
          .addAllProcess(procs.entrySet().stream()
              .map((e) -> AlluxioProcessStatus.newBuilder()
                  .setNodeType(e.getKey())
                  .setState(e.getValue())
                  .build())
              .collect(Collectors.toList()))
          .build());
    });
    return builder.build();
  }
}
