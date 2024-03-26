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
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerState;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParseException;
import io.etcd.jetcd.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * MembershipManager backed by configured etcd cluster. Only use active users
 */
public class ServiceRegistryMembershipManager implements MembershipManager {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceRegistryMembershipManager.class);
  private final AlluxioConfiguration mConf;
  private AlluxioEtcdClient mAlluxioEtcdClient;

  /**
   * @param conf
   * @return ServiceRegistryMembershipManager
   */
  public static ServiceRegistryMembershipManager create(AlluxioConfiguration conf) {
    return new ServiceRegistryMembershipManager(conf);
  }

  /**
   * CTOR for ServiceRegistryMembershipManager.
   * @param conf
   */
  public ServiceRegistryMembershipManager(AlluxioConfiguration conf) {
    this(conf, AlluxioEtcdClient.getInstance(conf));
  }

  /**
   * Default ServiceRegistryMembershipManager with given AlluxioEtcdClient client.
   * Only contains live workers.
   *
   * @param conf              Alluxio configuration
   * @param alluxioEtcdClient etcd client
   */
  public ServiceRegistryMembershipManager(AlluxioConfiguration conf,
      AlluxioEtcdClient alluxioEtcdClient) {
    mConf = conf;
    mAlluxioEtcdClient = alluxioEtcdClient;
  }

  @Override
  public void join(WorkerInfo workerInfo) throws IOException {
    LOG.info("Try joining Service Registry for worker:{} ", workerInfo);
    WorkerServiceEntity entity =
        new WorkerServiceEntity(workerInfo.getIdentity(), workerInfo.getAddress());
    entity.setLeaseTTLInSec(
        mConf.getDuration(PropertyKey.WORKER_FAILURE_DETECTION_TIMEOUT).getSeconds());
    mAlluxioEtcdClient.mServiceDiscovery.registerAndStartSync(entity);
    LOG.info("register to service registry for worker:{} ", workerInfo);
  }

  @Override
  public WorkerClusterView getAllMembers() throws IOException {
    return getLiveMembers();
  }

  @Override
  public WorkerClusterView getLiveMembers() throws IOException {
    Iterable<WorkerInfo> workerInfoIterable = parseWorkersFromEtcdKvPairs(
        mAlluxioEtcdClient.mServiceDiscovery.getAllLiveServices())
        .map(w -> new WorkerInfo()
            .setIdentity(w.getIdentity())
            .setAddress(w.getWorkerNetAddress())
            .setState(WorkerState.LIVE))
        ::iterator;
    return new WorkerClusterView(workerInfoIterable);
  }

  @Override
  public WorkerClusterView getFailedMembers() {
    return new WorkerClusterView(Collections.emptyList());
  }

  private Stream<WorkerServiceEntity> parseWorkersFromEtcdKvPairs(List<KeyValue> workerKvs) {
    return workerKvs
        .stream()
        .map(this::parseWorkerServiceEntity)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  private Optional<WorkerServiceEntity> parseWorkerServiceEntity(KeyValue etcdKvPair) {
    try {
      WorkerServiceEntity entity = new WorkerServiceEntity();
      entity.deserialize(etcdKvPair.getValue().getBytes());
      return Optional.of(entity);
    } catch (JsonParseException ex) {
      return Optional.empty();
    }
  }

  @Override
  @VisibleForTesting
  public String showAllMembers() {
    try {
      WorkerClusterView registeredWorkers = getAllMembers();
      String printFormat = "%s\t%s\t%s%n";
      StringBuilder sb = new StringBuilder(
          String.format(printFormat, "WorkerId", "Address", "Status"));
      for (WorkerInfo entity : registeredWorkers) {
        String entryLine = String.format(printFormat,
            entity.getIdentity(),
            entity.getAddress().getHost() + ":"
                + entity.getAddress().getRpcPort(), "ONLINE");
        sb.append(entryLine);
      }
      return sb.toString();
    } catch (IOException ex) {
      return String.format("Exception happened:%s", ex.getMessage());
    }
  }

  @Override
  @VisibleForTesting
  public void stopHeartBeat(WorkerInfo worker) throws IOException {
    WorkerServiceEntity entity = new WorkerServiceEntity(worker.getIdentity(), worker.getAddress());
    mAlluxioEtcdClient.mServiceDiscovery.unregisterService(entity.getServiceEntityName());
  }

  @Override
  public void decommission(WorkerInfo worker) throws IOException {
    // NOOP since we only have active workers
  }

  @Override
  public void close() throws Exception {
    // NOTHING TO CLOSE
    // The EtcdClient is a singleton so its life cycle is managed by the class itself
  }
}
