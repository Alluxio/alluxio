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

package alluxio.membership;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParseException;
import io.etcd.jetcd.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * MembershipManager backed by configured etcd cluster.
 */
public class EtcdMembershipManager implements MembershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(EtcdMembershipManager.class);
  private final AlluxioConfiguration mConf;
  private AlluxioEtcdClient mAlluxioEtcdClient;
  private String mClusterName;
  private Supplier<String> mRingPathPrefix =
      CommonUtils.memoize(this::constructRingPathPrefix);

  /**
   * @param conf
   * @return EtcdMembershipManager
   */
  public static EtcdMembershipManager create(AlluxioConfiguration conf) {
    return new EtcdMembershipManager(conf);
  }

  /**
   * CTOR for EtcdMembershipManager.
   * @param conf
   */
  public EtcdMembershipManager(AlluxioConfiguration conf) {
    this(conf, AlluxioEtcdClient.getInstance(conf));
  }

  /**
   * CTOR for EtcdMembershipManager with given AlluxioEtcdClient client.
   * @param conf
   * @param alluxioEtcdClient
   */
  public EtcdMembershipManager(AlluxioConfiguration conf, AlluxioEtcdClient alluxioEtcdClient) {
    mConf = conf;
    mClusterName = conf.getString(PropertyKey.ALLUXIO_CLUSTER_NAME);
    mAlluxioEtcdClient = alluxioEtcdClient;
  }

  private String constructRingPathPrefix() {
    return String.format("/DHT/%s/AUTHORIZED/", mClusterName);
  }

  private String getRingPathPrefix() {
    return mRingPathPrefix.get();
  }

  @Override
  public void join(WorkerInfo workerInfo) throws IOException {
    LOG.info("Try joining on etcd for worker:{} ", workerInfo);
    WorkerServiceEntity entity =
        new WorkerServiceEntity(workerInfo.getIdentity(), workerInfo.getAddress());
    // 1) register to the ring, check if there's existing entry
    String pathOnRing = new StringBuffer()
        .append(getRingPathPrefix())
        .append(entity.getServiceEntityName()).toString();
    byte[] existingEntityBytes = mAlluxioEtcdClient.getForPath(pathOnRing);
    byte[] serializedEntity = entity.serialize();
    // If there's existing entry, check if it's me.
    if (existingEntityBytes != null) {
      // It's not me, or not the same me.
      if (!Arrays.equals(existingEntityBytes, serializedEntity)) {
        // In k8s this might be bcos worker pod restarting with the same worker identity
        // but certain fields such as hostname has been changed. Register to ring path anyway.
        WorkerServiceEntity existingEntity = new WorkerServiceEntity();
        existingEntity.deserialize(existingEntityBytes);
        LOG.warn("Same worker entity found bearing same workerid:{},"
            + "existing WorkerServiceEntity to be overwritten:{},"
            + "maybe benign if pod restart in k8s env or same worker"
            + " scheduled to restart on another machine in baremetal env.",
            workerInfo.getIdentity().toString(), existingEntity);
        mAlluxioEtcdClient.createForPath(pathOnRing, Optional.of(serializedEntity));
      }
      // It's me, go ahead to start heartbeating.
    } else {
      // If haven't created myself onto the ring before, create now.
      mAlluxioEtcdClient.createForPath(pathOnRing, Optional.of(serializedEntity));
    }
    // 2) start heartbeat
    mAlluxioEtcdClient.mServiceDiscovery.registerAndStartSync(entity);
    LOG.info("Joined on etcd for worker:{} ", workerInfo);
  }

  @Override
  public WorkerClusterView getClusterView() throws IOException {
    return new EtcdWorkerClusterView();
  }

  class EtcdWorkerClusterView implements WorkerClusterView {
    private final Optional<ClusterViewFilter> mFilter;

    private EtcdWorkerClusterView() {
      mFilter = Optional.empty();
    }

    private EtcdWorkerClusterView(ClusterViewFilter filter) {
      mFilter = Optional.ofNullable(filter);
    }

    @Override
    public Optional<WorkerInfo> getWorkerById(WorkerIdentity toFind) {
      return getFilteredWorkers()
          .filter(w -> toFind.equals(w.getIdentity()))
          .findAny()
          .map(w -> new WorkerInfo()
              .setIdentity(w.getIdentity())
              .setAddress(w.getWorkerNetAddress()));
    }

    @Override
    public WorkerClusterView filter(ClusterViewFilter filter) {
      if (mFilter.isPresent()) {
        throw new UnsupportedOperationException("Cannot filter an already filtered view." +
            "Filter applied is " + mFilter.get());
      }
      return new EtcdWorkerClusterView(filter);
    }

    @Override
    public Iterator<WorkerInfo> iterator() {
      return getFilteredWorkers()
          .map(w -> new WorkerInfo()
              .setIdentity(w.getIdentity())
              .setAddress(w.getWorkerNetAddress()))
          .iterator();
    }

    private Stream<WorkerServiceEntity> retrieveFullMembers() {
      return mAlluxioEtcdClient.getChildren(getRingPathPrefix())
          .stream()
          .map(this::decode)
          .filter(Optional::isPresent)
          .map(Optional::get);
    }

    private Stream<WorkerServiceEntity> retrieveLiveMembers() {
      return mAlluxioEtcdClient.mServiceDiscovery
          .getAllLiveServices()
          .stream()
          .map(this::decode)
          .filter(Optional::isPresent)
          .map(Optional::get);
    }

    private Stream<WorkerServiceEntity> retrieveLostMembers(
        Stream<WorkerServiceEntity> liveMembers,
        Stream<WorkerServiceEntity> allMembers
    ) {
      Set<WorkerIdentity> liveWorkerIds = liveMembers
          .map(WorkerServiceEntity::getIdentity)
          .collect(Collectors.toSet());
      return allMembers
          .filter(w -> !liveWorkerIds.contains(w.getIdentity()));
    }

    private Stream<WorkerServiceEntity> getFilteredWorkers() {
      if (!mFilter.isPresent()) {
        return retrieveFullMembers();
      }
      Stream<WorkerServiceEntity> liveWorkers = retrieveLiveMembers();
      switch (mFilter.get()) {
        case LIVE:
          return liveWorkers;
        case LOST:
          return retrieveLostMembers(liveWorkers, retrieveFullMembers());
        default:
          throw new UnsupportedOperationException("Unknown filter: " + mFilter.get());
      }
    }

    private Optional<WorkerServiceEntity> decode(KeyValue etcdKvPair) {
      try {
        WorkerServiceEntity entity = new WorkerServiceEntity();
        entity.deserialize(etcdKvPair.getValue().getBytes());
        return Optional.of(entity);
      } catch (JsonParseException ex) {
        return Optional.empty();
      }
    }
  }

  @Override
  @VisibleForTesting
  public String showAllMembers() {
    try {
      WorkerClusterView registeredWorkers = getClusterView().snapshot();
      WorkerClusterView liveWorkers = getClusterView()
          .filter(ClusterViewFilter.LIVE)
          .snapshot();
      String printFormat = "%s\t%s\t%s%n";
      StringBuilder sb = new StringBuilder(
          String.format(printFormat, "WorkerId", "Address", "Status"));
      for (WorkerInfo entity : registeredWorkers) {
        String entryLine = String.format(printFormat,
            entity.getIdentity(),
            entity.getAddress().getHost() + ":"
                + entity.getAddress().getRpcPort(),
            liveWorkers.getWorkerById(entity.getIdentity()).isPresent() ? "ONLINE" : "OFFLINE");
        sb.append(entryLine);
      }
      return sb.toString();
    } catch (IOException ex) {
      return String.format("Exception happened:%s", ex.getMessage());
    }
  }

  @Override
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    WorkerServiceEntity entity = new WorkerServiceEntity(worker.getIdentity(), worker.getAddress());
    mAlluxioEtcdClient.mServiceDiscovery.unregisterService(entity.getServiceEntityName());
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    // TO BE IMPLEMENTED
  }

  @Override
  public void close() throws Exception {
    // NOTHING TO CLOSE
    // The EtcdClient is a singleton so its life cycle is managed by the class itself
  }
}
