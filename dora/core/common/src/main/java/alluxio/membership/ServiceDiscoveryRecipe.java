package alluxio.membership;

import alluxio.exception.status.AlreadyExistsException;
import com.google.common.base.Preconditions;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ServiceDiscoveryRecipe {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEtcdClient.class);
  private static final String BASE_PATH = "/ServiceDiscovery";
  Client mClient;
  AlluxioEtcdClient mAlluxioEtcdClient;
  String mClusterIdentifier = "";
  private final ReentrantLock mRegisterLock = new ReentrantLock();
  final ConcurrentHashMap<String, ServiceEntity> mRegisteredServices = new ConcurrentHashMap<>();
  public ServiceDiscoveryRecipe(AlluxioEtcdClient client, String clusterIdentifier) {
    mAlluxioEtcdClient = client;
    mAlluxioEtcdClient.connect();
    mClient = client.getEtcdClient();
    mClusterIdentifier = clusterIdentifier;
  }

  private String getRegisterPathPrefix() {
    return String.format("%s/%s", BASE_PATH, mClusterIdentifier);
  }

  @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
  public void registerAndStartSync(ServiceEntity service) throws IOException {
    LOG.info("registering service : {}", service);
    if (mRegisteredServices.containsKey(service.mServiceEntityName)) {
      throw new AlreadyExistsException("Service " + service.mServiceEntityName + " already registerd.");
    }
    String path = service.mServiceEntityName;
    String fullPath = getRegisterPathPrefix() + "/" + path;
    try {
      AlluxioEtcdClient.Lease lease = mAlluxioEtcdClient.createLease();
      Txn txn = mClient.getKVClient().txn();
      ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      service.serialize(dos);
      ByteSequence valToPut = ByteSequence.from(baos.toByteArray());
      CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.version(0L)))
          .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().withLeaseId(lease.mLeaseId).build()))
          .Then(Op.get(keyToPut, GetOption.DEFAULT))
          .Else(Op.get(keyToPut, GetOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      List<KeyValue> kvs = new ArrayList<>();
      txnResponse.getGetResponses().stream().map(
          r -> kvs.addAll(r.getKvs())).collect(Collectors.toList());
      if (!txnResponse.isSucceeded()) {
        if (!kvs.isEmpty()) {
          throw new AlreadyExistsException("Some process already registered same service and syncing,"
              + "this should not happen");
        }
        throw new IOException("Failed to register service:" + service.toString());
      }
      Preconditions.checkState(!kvs.isEmpty(), "No such service entry found.");
      long latestRevision = kvs.stream().mapToLong(kv -> kv.getModRevision()).max().getAsLong();
      service.mRevision = latestRevision;
      service.mLease = lease;
      startHeartBeat(service);
      mRegisteredServices.put(service.mServiceEntityName, service);
    } catch (ExecutionException ex) {
      throw new IOException("ExecutionException in registering service:" + service, ex);
    } catch (InterruptedException ex) {
      LOG.info("InterruptedException caught, bail.");
    }
  }

  @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
  public void unregisterService(String serviceIdentifier) throws IOException {
    if (!mRegisteredServices.containsKey(serviceIdentifier)) {
      LOG.info("Service {} already unregistered.", serviceIdentifier);
      return;
    }
    try (ServiceEntity service = mRegisteredServices.get(serviceIdentifier)) {
      boolean removed = mRegisteredServices.remove(serviceIdentifier, service);
      LOG.info("Unregister service {} : {}", service, (removed) ? "success" : "failed");
    }
  }

  public ByteBuffer getRegisteredServiceDetail(String serviceEntityName)
      throws IOException {
    String fullPath = getRegisterPathPrefix() + "/" + serviceEntityName;
    byte[] val = mAlluxioEtcdClient.getForPath(fullPath);
    return ByteBuffer.wrap(val);
  }

  @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
  public void updateService(ServiceEntity service) throws IOException {
    LOG.info("Updating service : {}", service);
    if (!mRegisteredServices.containsKey(service.mServiceEntityName)) {
      Preconditions.checkNotNull(service.mLease, "Service not attach with lease");
      throw new NoSuchElementException("Service " + service.mServiceEntityName
          + " not registered, please register first.");
    }
    String fullPath = getRegisterPathPrefix() + "/" + service.mServiceEntityName;
    try {
      Txn txn = mClient.getKVClient().txn();
      ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
      ByteSequence valToPut = ByteSequence.from(service.toString(), StandardCharsets.UTF_8);
      CompletableFuture<TxnResponse> txnResponseFut = txn
          .If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.modRevision(service.mRevision)))
          .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().withLeaseId(service.mLease.mLeaseId).build()))
          .Then(Op.get(keyToPut, GetOption.DEFAULT))
          .commit();
      TxnResponse txnResponse = txnResponseFut.get();
      // return if Cmp returns true
      if (!txnResponse.isSucceeded()) {
        throw new IOException("Failed to update service:" + service.toString());
      }
      startHeartBeat(service);
      mRegisteredServices.put(service.mServiceEntityName, service);
    } catch (ExecutionException ex) {
      throw new IOException("ExecutionException in registering service:" + service, ex);
    } catch (InterruptedException ex) {
      LOG.info("InterruptedException caught, bail.");
    }
  }

  private void startHeartBeat(ServiceEntity service) {
    service.setKeepAliveClient(mClient.getLeaseClient()
        .keepAlive(service.mLease.mLeaseId, new RetryKeepAliveObserver(service)));
  }

  class RetryKeepAliveObserver implements StreamObserver<LeaseKeepAliveResponse> {
    public ServiceEntity mService;
    public RetryKeepAliveObserver(ServiceEntity service) {
      mService = service;
    }
    @Override
    public void onNext(LeaseKeepAliveResponse value) {
      // NO-OP
    }

    @Override
    public void onError(Throwable t) {
      LOG.error("onError for Lease for service:{}, leaseId:{}, try starting new keepalive client..",
          mService, mService.mLease.mLeaseId, t);
      startHeartBeat(mService);
    }

    @Override
    public void onCompleted() {
      LOG.info("onCompleted for Lease for service:{}, leaseId:{}",
          mService, mService.mLease.mLeaseId);
    }
  }

  public Map<String, ByteBuffer> getAllLiveServices() {
    String clusterPath = getRegisterPathPrefix();
    Map<String, ByteBuffer> ret = new HashMap<>();
    List<KeyValue> children = mAlluxioEtcdClient.getChildren(clusterPath);
    for (KeyValue kv : children) {
      ret.put(kv.getKey().toString(StandardCharsets.UTF_8),
          ByteBuffer.wrap(kv.getValue().getBytes()));
    }
    return ret;
  }
}
