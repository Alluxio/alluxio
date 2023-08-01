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

import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.resource.LockResource;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ServiceDiscoveryRecipe for etcd, to track health status
 * of all registered services.
 */
public class ServiceDiscoveryRecipe {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceDiscoveryRecipe.class);
  private static final String BASE_PATH = "/ServiceDiscovery";
  final AlluxioEtcdClient mAlluxioEtcdClient;
  private final ScheduledExecutorService mExecutor;
  private final String mClusterIdentifier;
  // Will look like /ServiceDiscovery/<mClusterIdentifier>
  private final String mRegisterPathPrefix;
  private final ConcurrentHashMap<String, ServiceEntity> mRegisteredServices
      = new ConcurrentHashMap<>();

  /**
   * CTOR for ServiceDiscoveryRecipe.
   * @param client
   * @param clusterIdentifier
   */
  public ServiceDiscoveryRecipe(AlluxioEtcdClient client, String clusterIdentifier) {
    client.connect();
    mAlluxioEtcdClient = client;
    mClusterIdentifier = clusterIdentifier;
    mRegisterPathPrefix = String.format("%s%s%s", BASE_PATH,
        MembershipManager.PATH_SEPARATOR, mClusterIdentifier);
    mExecutor = Executors.newSingleThreadScheduledExecutor(
        ThreadFactoryUtils.build("service-discovery-checker", false));
    mExecutor.scheduleWithFixedDelay(this::checkAllForReconnect,
        AlluxioEtcdClient.DEFAULT_LEASE_TTL_IN_SEC, AlluxioEtcdClient.DEFAULT_LEASE_TTL_IN_SEC,
        TimeUnit.SECONDS);
  }

  /**
   * Apply for a new lease or extend expired lease for
   * given ServiceEntity in atomic fashion.
   * Atomicity:
   * creation of given ServiceEntity entry on etcd is handled by etcd transaction
   * iff the version = 0 which means when there's no such key present.
   * (expired lease will automatically delete the kv attached with it on etcd)
   * update of the ServiceEntity fields(lease,revision num) is guarded by
   * lock within ServiceEntity instance.
   * @param service
   * @throws IOException
   */
  private void newLeaseInternal(ServiceEntity service) throws IOException {
    try (LockResource lockResource = new LockResource(service.mLock)) {
      if (service.mLease != null && !mAlluxioEtcdClient.isLeaseExpired(service.mLease)) {
        LOG.info("Lease attached with service:{} is not expired, bail from here.");
        return;
      }
      String path = service.mServiceEntityName;
      String fullPath = new StringBuffer().append(mRegisterPathPrefix)
          .append(MembershipManager.PATH_SEPARATOR)
          .append(path).toString();
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        AlluxioEtcdClient.Lease lease = mAlluxioEtcdClient.createLease();
        Txn txn = mAlluxioEtcdClient.getEtcdClient().getKVClient().txn();
        ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
        DataOutputStream dos = new DataOutputStream(baos);
        service.serialize(dos);
        ByteSequence valToPut = ByteSequence.from(baos.toByteArray());
        CompletableFuture<TxnResponse> txnResponseFut = txn
            .If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.version(0L)))
            .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder()
                .withLeaseId(lease.mLeaseId).build()))
            .Then(Op.get(keyToPut, GetOption.DEFAULT))
            .Else(Op.get(keyToPut, GetOption.DEFAULT))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        List<KeyValue> kvs = new ArrayList<>();
        txnResponse.getGetResponses().stream().map(
            r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
        if (!txnResponse.isSucceeded()) {
          if (!kvs.isEmpty()) {
            throw new AlreadyExistsException("Same service kv pair is there but "
                + "attached lease is expired, this should not happen");
          }
          throw new IOException("Failed to new a lease for service:" + service.toString());
        }
        Preconditions.checkState(!kvs.isEmpty(), "No such service entry found.");
        long latestRevision = kvs.stream().mapToLong(kv -> kv.getModRevision())
            .max().getAsLong();
        service.mRevision = latestRevision;
        service.mLease = lease;
        startHeartBeat(service);
      } catch (ExecutionException | InterruptedException ex) {
        throw new IOException("Exception in new-ing lease for service:" + service, ex);
      }
    }
  }

  /**
   * Register service and start keeping-alive.
   * Atomicity:
   * So the same-named ServiceEntity registration atomicity on etcd is guaranteed
   * in {@link ServiceDiscoveryRecipe#newLeaseInternal(ServiceEntity)},
   * by etcd transaction semantics. We ensure that
   * if #newLeaseInternal succeeded, it's safe to track in mRegisteredServices map.
   * Other threads within same process or other processes trying to
   * register same named service will fail in #newLeaseInternal already.
   * @param service
   * @throws IOException
   */
  public void registerAndStartSync(ServiceEntity service) throws IOException {
    LOG.info("registering service : {}", service);
    if (mRegisteredServices.containsKey(service.getServiceEntityName())) {
      throw new AlreadyExistsException("Service " + service.mServiceEntityName
          + " already registered.");
    }
    newLeaseInternal(service);
    ServiceEntity existEntity = mRegisteredServices.putIfAbsent(
        service.getServiceEntityName(), service);
    if (existEntity != null) {
      // We should never reach here as if concurrent new lease creation for service
      // on etcd will not succeed for both race parties.
      try (ServiceEntity entity = service) {
        // someone is already in register service map, close myself before throw exception.
      }
      throw new AlreadyExistsException("Service " + service.mServiceEntityName
          + " already registered.");
    }
  }

  /**
   * Unregister service and close corresponding keepalive client if any.
   * @param serviceIdentifier
   * @throws IOException
   */
  public void unregisterService(String serviceIdentifier) throws IOException {
    ServiceEntity entity = mRegisteredServices.remove(serviceIdentifier);
    if (entity != null) {
      // It is ok to ignore the declared IOException from closing
      // removed ServiceEntity from the map. As internal resource
      // closing doesn't throw IOException at all.
      try (ServiceEntity service = entity) {
        LOG.info("Service unregistered:{}", service);
      }
    } else {
      LOG.info("Service already unregistered:{}", serviceIdentifier);
    }
  }

  /**
   * Unregister all services registered from this ServiceDiscoveryRecipe instance.
   * [It won't register services registered through other instances(other processes)]
   */
  public void unregisterAll() {
    for (Map.Entry<String, ServiceEntity> entry : mRegisteredServices.entrySet()) {
      try {
        unregisterService(entry.getKey());
      } catch (IOException ex) {
        LOG.error("Unregister all services failed unregistering for:{}.", entry.getKey(), ex);
      }
    }
  }

  /**
   * Get the registered service value as ByteBuffer.
   * @param serviceEntityName
   * @return ByteBuffer container serialized content
   * @throws IOException
   */
  public ByteBuffer getRegisteredServiceDetail(String serviceEntityName)
      throws IOException {
    String fullPath = new StringBuffer().append(mRegisterPathPrefix)
        .append(MembershipManager.PATH_SEPARATOR)
        .append(serviceEntityName).toString();
    byte[] val = mAlluxioEtcdClient.getForPath(fullPath);
    return ByteBuffer.wrap(val);
  }

  /**
   * Update the service value with new value.
   * TODO(lucy) we need to handle the cases where txn failed bcos of
   * lease expiration.
   * Atomicity:
   * update of given ServiceEntity on etcd is handled by etcd transaction
   * on comparing the revision number for a CAS semantic update.
   * update of the ServiceEntity fields is guarded by update lock within
   * ServiceEntity instance.
   * @param service
   * @throws IOException
   */
  public void updateService(ServiceEntity service) throws IOException {
    LOG.info("Updating service : {}", service);
    if (!mRegisteredServices.containsKey(service.mServiceEntityName)) {
      Preconditions.checkNotNull(service.mLease, "Service not attach with lease");
      throw new NoSuchElementException("Service " + service.mServiceEntityName
          + " not registered, please register first.");
    }
    String fullPath = new StringBuffer().append(mRegisterPathPrefix)
        .append(MembershipManager.PATH_SEPARATOR)
        .append(service.mServiceEntityName).toString();
    try (LockResource lockResource = new LockResource(service.mLock);
         ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Txn txn = mAlluxioEtcdClient.getEtcdClient().getKVClient().txn();
      ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
      DataOutputStream dos = new DataOutputStream(baos);
      service.serialize(dos);
      ByteSequence valToPut = ByteSequence.from(baos.toByteArray());
      CompletableFuture<TxnResponse> txnResponseFut = txn
          .If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.modRevision(service.mRevision)))
          .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder()
              .withLeaseId(service.mLease.mLeaseId).build()))
          .Then(Op.get(keyToPut, GetOption.DEFAULT))
          .Else(Op.get(keyToPut, GetOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      List<KeyValue> kvs = new ArrayList<>();
      txnResponse.getGetResponses().stream().map(
          r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
      // return if Cmp returns true
      if (!txnResponse.isSucceeded()) {
        if (kvs.isEmpty()) {
          throw new NotFoundException("Such service kv pair is not in etcd anymore.");
        }
        throw new IOException("Failed to update service:" + service.toString());
      }
      // update the service with
      long latestRevision = kvs.stream().mapToLong(kv -> kv.getModRevision())
          .max().getAsLong();
      service.mRevision = latestRevision;
      if (service.getKeepAliveClient() == null) {
        startHeartBeat(service);
      }
      // This should be a no-op, as the we should not overwrite any other values.
      mRegisteredServices.put(service.getServiceEntityName(), service);
    } catch (ExecutionException ex) {
      throw new IOException("ExecutionException in registering service:" + service, ex);
    } catch (InterruptedException ex) {
      LOG.info("InterruptedException caught, bail.");
    }
  }

  /**
   * Start heartbeating(keepalive) for the given service.
   * @param service
   */
  private void startHeartBeat(ServiceEntity service) {
    CloseableClient keepAliveClient = mAlluxioEtcdClient.getEtcdClient().getLeaseClient()
        .keepAlive(service.mLease.mLeaseId, new RetryKeepAliveObserver(service));
    service.setKeepAliveClient(keepAliveClient);
  }

  class RetryKeepAliveObserver implements StreamObserver<LeaseKeepAliveResponse> {
    public ServiceEntity mService;

    public RetryKeepAliveObserver(ServiceEntity service) {
      mService = service;
    }

    @Override
    public void onNext(LeaseKeepAliveResponse value) {
      // NO-OP
      LOG.debug("onNext keepalive response:id:{}:ttl:{}", value.getID(), value.getTTL());
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("onError for Lease for service:{}, leaseId:{}. Setting status to reconnect",
          mService, mService.mLease.mLeaseId, t);
      mService.mNeedReconnect.compareAndSet(false, true);
    }

    @Override
    public void onCompleted() {
      LOG.warn("onCompleted for Lease for service:{}, leaseId:{}. Setting status to reconnect",
          mService, mService.mLease.mLeaseId);
      mService.mNeedReconnect.compareAndSet(false, true);
    }
  }

  /**
   * Get all healthy service list.
   * @return return service name to service entity serialized value
   */
  public Map<String, ByteBuffer> getAllLiveServices() throws IOException {
    Map<String, ByteBuffer> ret = new HashMap<>();
    List<KeyValue> children = mAlluxioEtcdClient.getChildren(mRegisterPathPrefix);
    for (KeyValue kv : children) {
      ret.put(kv.getKey().toString(StandardCharsets.UTF_8),
          ByteBuffer.wrap(kv.getValue().getBytes()));
    }
    return ret;
  }

  /**
   * Periodically check if any ServiceEntity's lease got expired and needs
   * to renew the lease with new keepalive client.
   */
  private void checkAllForReconnect() {
    // No need for lock over all services, just individual ServiceEntity is enough
    for (Map.Entry<String, ServiceEntity> entry : mRegisteredServices.entrySet()) {
      ServiceEntity entity = entry.getValue();
      try (LockResource lockResource = new LockResource(entry.getValue().mLock)) {
        if (entity.mNeedReconnect.get()) {
          try {
            LOG.info("Start reconnect for service:{}", entity.getServiceEntityName());
            newLeaseInternal(entity);
            entity.mNeedReconnect.set(false);
          } catch (IOException e) {
            LOG.info("Failed trying to new the lease for service:{}", entity, e);
          }
        }
      }
    }
  }
}
