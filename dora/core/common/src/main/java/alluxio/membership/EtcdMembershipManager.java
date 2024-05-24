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
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerState;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParseException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
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
    entity.setLeaseTTLInSec(
        mConf.getDuration(PropertyKey.WORKER_FAILURE_DETECTION_TIMEOUT).getSeconds());
    String pathOnRing = new StringBuffer()
        .append(getRingPathPrefix())
        .append(entity.getServiceEntityName()).toString();
    byte[] serializedEntity = entity.serialize();
    // 1) register to the ring.
    // CompareAndSet if no existing registered entry, if exist such key, two cases:
    // a) it's k8s env, still register, overwriting the existing entry
    // b) it's not k8s env, compare the registered entity content, if it's me
    //    then no op, if not, we don't allow overwriting the existing entity.
    try {
      boolean isK8s = mConf.isSet(PropertyKey.K8S_ENV_DEPLOYMENT)
          && mConf.getBoolean(PropertyKey.K8S_ENV_DEPLOYMENT);
      Txn txn = mAlluxioEtcdClient.getEtcdClient().getKVClient().txn();
      ByteSequence keyToPut = ByteSequence.from(pathOnRing, StandardCharsets.UTF_8);
      ByteSequence valToPut = ByteSequence.from(serializedEntity);
      CompletableFuture<TxnResponse> txnResponseFut = txn
          // version of the key indicates number of modification, 0 means
          // this key does not exist
          .If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.version(0L)))
          .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().build()))
          .Else(isK8s ? Op.put(keyToPut, valToPut, PutOption.newBuilder().build()) :
              Op.get(keyToPut, GetOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      if (!isK8s && !txnResponse.isSucceeded()) {
        // service kv already exists, for non-k8s env, check if it's me.
        // bail if it's someone else.
        List<KeyValue> kvs = new ArrayList<>();
        txnResponse.getGetResponses().stream().map(
            r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
        Optional<KeyValue> latestKV = kvs.stream()
            .max((kv1, kv2) -> (int) (kv1.getModRevision() - kv2.getModRevision()));
        if (latestKV.isPresent()
            && !Arrays.equals(latestKV.get().getValue().getBytes(), serializedEntity)) {
          Optional<WorkerServiceEntity> existingEntity = parseWorkerServiceEntity(latestKV.get());
          if (!existingEntity.isPresent()) {
            throw new IOException(String.format(
                "Existing WorkerServiceEntity for path:%s corrupted",
                pathOnRing));
          }
          if (existingEntity.get().equalsIgnoringOptionalFields(entity)) {
            // Same entity but potentially with new optional fields,
            // update the original etcd-stored worker information
            mAlluxioEtcdClient.createForPath(pathOnRing, Optional.of(serializedEntity));
          } else {
            throw new AlreadyExistsException(
                String.format("Some other member with same id registered on the ring, bail."
                        + "Conflicting worker addr:%s, worker identity:%s."
                        + "Different workers can't assume same worker identity in non-k8s env,"
                        + "clean local worker identity settings to continue.",
                    existingEntity.get().getWorkerNetAddress().toString(),
                    existingEntity.get().getIdentity()));
          }
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }
    // 2) start heartbeat
    mAlluxioEtcdClient.mServiceDiscovery.registerAndStartSync(entity);
    LOG.info("Joined on etcd for worker:{} ", workerInfo);
  }

  @Override
  public WorkerClusterView getAllMembers() throws IOException {
    Set<WorkerIdentity> liveWorkerIds = parseWorkersFromEtcdKvPairs(
        mAlluxioEtcdClient.mServiceDiscovery.getAllLiveServices())
        .map(WorkerServiceEntity::getIdentity)
        .collect(Collectors.toSet());
    Predicate<WorkerInfo> isLive = w -> liveWorkerIds.contains(w.getIdentity());
    Iterable<WorkerInfo> workerInfoIterable = parseWorkersFromEtcdKvPairs(
        mAlluxioEtcdClient.getChildren(getRingPathPrefix()))
        .map(w -> new WorkerInfo()
            .setIdentity(w.getIdentity())
            .setAddress(w.getWorkerNetAddress()))
        .map(w -> w.setState(isLive.test(w) ? WorkerState.LIVE : WorkerState.LOST))
        ::iterator;
    return new WorkerClusterView(workerInfoIterable);
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
  public WorkerClusterView getFailedMembers() throws IOException {
    Set<WorkerIdentity> liveWorkerIds = parseWorkersFromEtcdKvPairs(
        mAlluxioEtcdClient.mServiceDiscovery.getAllLiveServices())
        .map(WorkerServiceEntity::getIdentity)
        .collect(Collectors.toSet());
    Iterable<WorkerInfo> failedWorkerIterable = parseWorkersFromEtcdKvPairs(
        mAlluxioEtcdClient.getChildren(getRingPathPrefix()))
        .filter(w -> !liveWorkerIds.contains(w.getIdentity()))
        .map(w -> new WorkerInfo()
            .setIdentity(w.getIdentity())
            .setAddress(w.getWorkerNetAddress())
            .setState(WorkerState.LOST))
        ::iterator;
    return new WorkerClusterView(failedWorkerIterable);
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
      WorkerClusterView liveWorkers = getLiveMembers();
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
    Optional<WorkerInfo> targetWorker = getAllMembers().getWorkerById(worker.getIdentity());
    if (!targetWorker.isPresent()) {
      throw new InvalidArgumentException(
          String.format("Unrecognized or non-existing worker: %s", worker.getIdentity()));
    }
    // Worker should already be offline
    if (targetWorker.get().getState() != WorkerState.LOST) {
      throw new InvalidArgumentException(
          String.format("Can't remove running worker: %s, stop the worker"
              + " before removing", worker.getIdentity()));
    }
    // stop heartbeat if it is an existing service discovery tab(although unlikely)
    stopHeartBeat(worker);
    String pathOnRing = new StringBuffer()
        .append(getRingPathPrefix())
        .append(worker.getIdentity()).toString();
    mAlluxioEtcdClient.deleteForPath(pathOnRing, false);
    LOG.info("Successfully removed worker:{}", worker.getIdentity());
  }

  @Override
  public void close() throws Exception {
    // NOTHING TO CLOSE
    // The EtcdClient is a singleton so its life cycle is managed by the class itself
  }
}
