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

import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.hub.common.RpcClient;
import alluxio.hub.proto.AgentManagerServiceGrpc;
import alluxio.hub.proto.AlluxioNodeType;
import alluxio.hub.proto.HubNodeAddress;
import alluxio.hub.proto.HubNodeState;
import alluxio.hub.proto.HubNodeStatus;
import alluxio.retry.CountingRetry;
import alluxio.util.LogUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A class representing a set of Hub Agent nodes registered with the manager.
 */
@ThreadSafe
public class HubCluster implements ProtoConverter<alluxio.hub.proto.HubCluster>, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(HubCluster.class);

  private final EventListeningConcurrentHashMap<HubNodeAddress, HubNodeState> mNodes;
  private final ConcurrentHashMap<HubNodeAddress, Long> mHeartbeats;
  private final ConcurrentHashMap<HubNodeAddress,
      RpcClient<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub>> mClients;
  private final long mLostThreshold;
  private final long mDeleteThreshold;
  private final Future<?> mScanner;
  private final AlluxioCluster mAlluxioCluster;

  /**
   * Exposed for testing.
   *
   * @param svc the executor service to schedule node state detection
   * @param eventListenerService the executor to run events on
   * @param lostThreshold see {@link #HubCluster(ScheduledExecutorService, long, long
   * , AlluxioCluster)}
   * @param deleteThreshold see {@link #HubCluster(ScheduledExecutorService, long, long
   * , AlluxioCluster)}
   * @param alluxioCluster the alluxio cluster the hub is managing
   */
  HubCluster(ScheduledExecutorService svc,
             ExecutorService eventListenerService, long lostThreshold,
             long deleteThreshold, AlluxioCluster alluxioCluster) {
    Preconditions.checkArgument(lostThreshold < deleteThreshold,
        "lost threshold must be > delete threshold");
    mDeleteThreshold = deleteThreshold;
    mLostThreshold = lostThreshold;
    // We only need to scan at minimum based on the lost threshold. If the threshold isn't a
    // multiple of the delete threshold, then it may appear that deletes take longer. They will
    // eventually get deleted, which is fine.
    mScanner = svc.scheduleAtFixedRate(this::scanNodes, 0, lostThreshold,
            TimeUnit.MILLISECONDS);
    mNodes = new EventListeningConcurrentHashMap<>(eventListenerService);
    mHeartbeats = new ConcurrentHashMap<>();
    mClients = new ConcurrentHashMap<>();
    mAlluxioCluster = alluxioCluster;
    mNodes.registerEventListener(() -> {
    }, 1000);
  }

  /**
   * Creates a new instance of {@link HubCluster}.
   *
   * @param svc the executor service used to run event handlers
   * @param lostThreshold the amount of time before an agent should be considered
   *        {@link HubNodeState#LOST}
   * @param deleteThreshold the amount fo time before an agent should be removed from the cluster
   */
  public HubCluster(ScheduledExecutorService svc, long lostThreshold,
                    long deleteThreshold) {
    this(svc, svc, lostThreshold, deleteThreshold, new AlluxioCluster((svc)));
  }

  /**
   * Creates a new instance of {@link HubCluster}.
   *
   * @param svc the executor service used to run event handlers
   * @param lostThreshold the amount of time before an agent should be considered
   *        {@link HubNodeState#LOST}
   * @param deleteThreshold the amount fo time before an agent should be removed from the cluster
   * @param alluxioCluster the alluxio cluster that the hub cluster is managing
   */
  public HubCluster(ScheduledExecutorService svc, long lostThreshold,
                    long deleteThreshold, AlluxioCluster alluxioCluster) {
    this(svc, svc, lostThreshold, deleteThreshold, alluxioCluster);
  }

  /**
   * Add a new node to the cluster.
   *
   * @param addr the given address
   * @throws IllegalArgumentException when either the hostname or port do not exist
   */
  public void add(HubNodeAddress addr) {
    Preconditions.checkArgument(addr.hasHostname(), "node address must have hostname");
    Preconditions.checkArgument(addr.hasRpcPort(), "node address must have port");
    mNodes.put(addr, HubNodeState.ALIVE);
  }

  /**
   * heartbeats to update the state of the node with the given address.
   *
   * @param addr the address to heartbeat
   */
  public void heartbeat(HubNodeAddress addr) {
    Preconditions.checkArgument(addr.hasHostname(), "node address must have hostname");
    Preconditions.checkArgument(addr.hasRpcPort(), "node address must have port");
    mHeartbeats.put(addr, System.currentTimeMillis());
    if (!mNodes.containsKey(addr)) {
      add(addr);
    }
  }

  /**
   * Scans over the list of nodes in the cluster and marks them as lost if they haven't sent a
   * heartbeat in the expected interval. Otherwise, if the node is in the
   * {@link HubNodeState#LOST} and the time there exceeds a threshold, consider the node gone and
   * remove it from the cluster.
   */
  void scanNodes() {
    long now = System.currentTimeMillis();
    long deleteIfBefore = now - mDeleteThreshold;
    long lostIfBefore = now - mLostThreshold;
    Iterator<Map.Entry<HubNodeAddress, Long>> iter = mHeartbeats.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<HubNodeAddress, Long> entry = iter.next();
      HubNodeAddress addr = entry.getKey();
      long time = entry.getValue();
      if (time < deleteIfBefore) {
        if (!remove(addr)) {
          LOG.debug("Failed to remove node {} from cluster", addr);
        }
        iter.remove();
        mAlluxioCluster.remove(addr.getHostname());
      } else if (time < lostIfBefore) {
        mNodes.put(addr, HubNodeState.LOST);
      } else {
        mNodes.put(addr, HubNodeState.ALIVE);
      }
    }
  }

  /**
   * Removes a node from the cluster.
   *
   * @param addr the node address to remove
   * @return true if the host was removed, false otherwise
   */
  public boolean remove(HubNodeAddress addr) {
    mClients.remove(addr);
    return mNodes.remove(addr) != null;
  }

  /**
   * @return the number of registered nodes
   */
  public int size() {
    return mNodes.size();
  }

  /**
   * @return a set of all hosts in the cluster
   */
  public Set<String> hosts() {
    return mNodes.keySet().stream().map(HubNodeAddress::getHostname).collect(Collectors.toSet());
  }

  /**
   * Execute an RPC action on all nodes in the cluster.
   *
   * @param hosts the hosts to execute on
   * @param conf alluxio configuration
   * @param action the client action to perform
   * @param svc an executor service to run the RPCs in parallel
   * @param <T> the type of response from the RPC function
   * @return a map of the hosts and their respective responses from the {@code action} argument
   * @throws RuntimeException when the RPC client fail to be created or any of the RPCs fail
   */
  public <T> Map<HubNodeAddress, T> exec(Set<HubNodeAddress> hosts,
      AlluxioConfiguration conf,
      Function<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub, T> action,
      ExecutorService svc) throws RuntimeException {
    // get all of the non-lost nodes which are in the hosts set then, make sure we create an RPC
    // client for each of those. Finally, execute the RPC calls in parallel.
    return mNodes.entrySet().stream()
        .filter(e -> e.getValue() != HubNodeState.LOST && hosts.contains(e.getKey()))
        .map(e -> {
          RpcClient<AgentManagerServiceGrpc.AgentManagerServiceBlockingStub> c =
              mClients.computeIfAbsent(e.getKey(), (hubNode) -> new RpcClient<>(conf,
              new InetSocketAddress(hubNode.getHostname(), hubNode.getRpcPort()),
              AgentManagerServiceGrpc::newBlockingStub, () -> new CountingRetry(2)));
          return new Pair<>(e.getKey(), c);
        })
        .map(pair -> {
          try {
            return new Pair<>(pair.getFirst(), pair.getSecond().get());
          } catch (AlluxioStatusException e) {
            LOG.warn("Failed to connect to {}", pair.getSecond().getAddress());
            throw new RuntimeException("Failed to connect to " + pair.getSecond().getAddress());
          }
        })
        .map(pair -> new Pair<>(pair.getFirst(), svc.submit(() -> action.apply(pair.getSecond()))))
        .map(pair -> {
          try {
            return new Pair<>(pair.getFirst(), pair.getSecond().get(1, TimeUnit.HOURS));
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LogUtils.warnWithException(LOG, "Failed to send RPC to {}", pair.getFirst(), e);
            throw new RuntimeException("Failed to execute action on " + pair.getFirst(), e);
          }
        })
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  /**
   * Returns a set of Hub nodes corresponding to the given {@link AlluxioNodeType} from an
   * {@link AlluxioCluster}.
   *
   * @param cluster the alluxio cluster to query from
   * @param type the type of Alluxio node to query for
   * @return a set of Hub nodes co-located with Alluxio processes of the given type
   */
  public Set<HubNodeAddress> nodesFromAlluxio(AlluxioCluster cluster, AlluxioNodeType type) {
    Set<String> alluxioHosts = cluster.getNodeType(type);
    return mNodes.entrySet().stream()
        .filter(e -> e.getValue() != HubNodeState.LOST
            && alluxioHosts.contains(e.getKey().getHostname()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());
  }

  /**
   * @return a set containing all live hub nodes
   */
  public Set<HubNodeAddress> allNodes() {
    return mNodes.entrySet().stream()
        .filter(e -> e.getValue() != HubNodeState.LOST)
        .map(Map.Entry::getKey).collect(Collectors.toSet());
  }

  /**
   * Get alluxio cluster.
   *
   * @return alluxio cluster
   */
  public AlluxioCluster getAlluxioCluster() {
    return mAlluxioCluster;
  }

  @Override
  public alluxio.hub.proto.HubCluster toProto() {
    return alluxio.hub.proto.HubCluster.newBuilder()
        .addAllNode(mNodes.entrySet().stream().map((e) -> HubNodeStatus.newBuilder()
            .setNode(e.getKey())
            .setState(e.getValue())
            .build())
            .collect(Collectors.toList()))
        .build();
  }

  @Override
  public void close() {
    mScanner.cancel(true);
  }
}
