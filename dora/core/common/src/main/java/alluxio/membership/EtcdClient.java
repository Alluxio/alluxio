package alluxio.membership;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.Worker;
import com.google.common.base.MoreObjects;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Txn;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.support.Observers;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public class EtcdClient {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

  protected AtomicBoolean mConnected = new AtomicBoolean(false);
  private Client mEtcdClient;

  public EtcdClient() {

  }

  public void connect() {
    if (mConnected.get()) {
      return;
    }
    List<String> endpoints = new ArrayList<>();

    // create client using endpoints
    Client client = Client.builder().endpoints(
        "http://localhost:2379" //, "http://etcd1:2379", "http://etcd2:2379"
        )
        .build();
    if (mConnected.compareAndSet(false, true)) {
      mEtcdClient = client;
    }
  }

  public void disconnect() {

  }

  public Client getEtcdClient() {
    if (mConnected.get()) {
      return mEtcdClient;
    }
    connect();
    return mEtcdClient;
  }

  public static void main(String[] args) {
    try {
      EtcdClient etcdClient = new EtcdClient();
      etcdClient.connect();
      String clusterId = UUID.randomUUID().toString();
      ServiceDiscoveryRecipe sd = new ServiceDiscoveryRecipe(etcdClient.getEtcdClient(),
          clusterId, 2L);
      WorkerService service = new WorkerService();
      service.mAddress = new WorkerNetAddress()
          .setHost(NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.WORKER_RPC,
              Configuration.global()))
          .setContainerHost(Configuration.global()
              .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
          .setRpcPort(1234)
          .setDataPort(2234)
          .setWebPort(3344);
      service.mWorkerId = new AtomicReference<Long>(12L);
      sd.registerService(service);
      sd.getAllServices();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  public static class ServiceEntityContext implements Closeable {
    CloseableClient mKeepAliveClient;
    Long mLeaseId;
    String mIdentifierName;
    protected ServiceEntityContext(String identifierName) {
      mIdentifierName = identifierName;
    }

    @Override
    public void close() throws IOException {
      if (mKeepAliveClient != null) {
        mKeepAliveClient.close();
      }
    }
  }

  public static class ServiceDiscoveryRecipe {
    String basePath = "/ServiceDiscovery";
    Client mClient;
    String mClusterIdentifier;
    final long mLeaseTtlInSec;
    private final ReentrantLock mRegisterLock = new ReentrantLock();
    final ConcurrentHashMap<String, ServiceEntityContext> mRegisteredServices = new ConcurrentHashMap<>();
    ServiceDiscoveryRecipe(Client client, String clusterIdentifier, long leaseTtlSec) {
      mClient = client;
      mClusterIdentifier = clusterIdentifier;
      mLeaseTtlInSec = leaseTtlSec;
    }

    @GuardedBy("ServiceDiscoveryRecipe#mRegisterLock")
    public void registerService(ServiceEntityContext service) throws IOException {
      if (mRegisteredServices.containsKey(service.mIdentifierName)) {
        throw new AlreadyExistsException("Service " + service.mIdentifierName + " already registerd.");
      }
      String path = service.mIdentifierName;
      String fullPath = basePath + "/" + mClusterIdentifier + "/" + path;
      CompletableFuture<LeaseGrantResponse> leaseGrantFut =
          mClient.getLeaseClient().grant(0, mLeaseTtlInSec, TimeUnit.SECONDS);
      // retry
      long leaseId;
      try {
        LeaseGrantResponse resp = leaseGrantFut.get();
        leaseId = resp.getID();
        Txn txn = mClient.getKVClient().txn();
        ByteSequence keyToPut = ByteSequence.from(fullPath, StandardCharsets.UTF_8);
        ByteSequence valToPut = ByteSequence.from(service.toString(), StandardCharsets.UTF_8);
        CompletableFuture<TxnResponse> txnResponseFut = txn.If(new Cmp(keyToPut, Cmp.Op.EQUAL, CmpTarget.version(0L)))
            .Then(Op.put(keyToPut, valToPut, PutOption.newBuilder().withLeaseId(leaseId).build()))
            .commit();
        TxnResponse txnResponse = txnResponseFut.get();
        if (!txnResponse.isSucceeded()) {
          throw new IOException("Failed to register service:" + service.toString());
        }
        service.mLeaseId = leaseId;
        startHeartBeat(service);
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
      }
      try (ServiceEntityContext service = mRegisteredServices.get(serviceIdentifier)) {
        
      }
    }

    StreamObserver<LeaseKeepAliveResponse> mKeepAliveObserver = new StreamObserver<LeaseKeepAliveResponse>() {
      @Override
      public void onNext(LeaseKeepAliveResponse value) {
        LOG.info("onNext:id:{}:ttl:{}", value.getID(), value.getTTL());
      }

      @Override
      public void onError(Throwable t) {
        LOG.info("onError:{}", t);
      }

      @Override
      public void onCompleted() {
        LOG.info("onCompleted");
      }
    };

    public void startHeartBeat(ServiceEntityContext service) {
      if (service.mLeaseId != -1L) {
        service.mKeepAliveClient = mClient.getLeaseClient()
            .keepAlive(service.mLeaseId, mKeepAliveObserver);
      }
    }

    public void getAllServices() {
      String clusterPath = basePath + "/" + mClusterIdentifier;
      try {
        GetResponse getResponse = mClient.getKVClient()
            .get(ByteSequence.from(clusterPath, StandardCharsets.UTF_8),
                GetOption.newBuilder().isPrefix(true).build())
            .get();
        List<KeyValue> kvs = getResponse.getKvs();
        LOG.info("[LUCY]:kvs:path:{}", clusterPath);
        for (KeyValue kv : kvs) {
          LOG.info("[LUCY]k:{}:v:{}:version:{}:createVersion:{}:modifyVersion:{}:lease:{}",
              kv.getKey().toString(StandardCharsets.UTF_8), kv.getValue().toString(StandardCharsets.UTF_8),
              kv.getVersion(), kv.getCreateRevision(), kv.getModRevision(), kv.getLease());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

  }

}
