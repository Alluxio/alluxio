package alluxio.worker.dora;


import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.membership.MembershipManager;
import com.google.common.io.Closer;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

//@Testcontainers
public class TestWorkerMembership {

  private static final Network network = Network.newNetwork();
  private static final int ETCD_PORT = 2379;

  private static ToxiproxyContainer.ContainerProxy etcdProxy;

  @AfterClass
  public static void afterAll() {
    network.close();
  }

  @ClassRule
  public static final GenericContainer<?> etcd =
      new GenericContainer<>("quay.io/coreos/etcd:latest")
          .withCommand("etcd",
              "--listen-client-urls", "http://0.0.0.0:" + ETCD_PORT,
              "--advertise-client-urls", "http://0.0.0.0:" + ETCD_PORT)
          .withExposedPorts(ETCD_PORT)
          .withNetwork(network);

  @ClassRule
  public static final ToxiproxyContainer toxiproxy =
      new ToxiproxyContainer(
//          "shopify/toxiproxy:2.1.0")
          "ghcr.io/shopify/toxiproxy:2.5.0")
          .withNetwork(network)
          .withNetworkAliases("toxiproxy");

  private PagedDoraWorker mWorker;
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void beforeEach() throws Exception {
    etcdProxy = toxiproxy.getProxy(etcd, ETCD_PORT);

//    Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
//        mTestFolder.newFolder("rocks"));
//    CacheManagerOptions cacheManagerOptions =
//        CacheManagerOptions.createForWorker(Configuration.global());
//
//    PageMetaStore pageMetaStore =
//        PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
//    mCacheManager =
//        CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
//    mMembershipManager =
//        MembershipManager.Factory.create(Configuration.global());
//    mWorker = new PagedDoraWorker(new AtomicReference<>(1L),
//        Configuration.global(), mCacheManager, mMembershipManager);
  }

  private List<URI> getClientEndpoints() {
    return List.of(URI.create(
        "https://" + etcd.getContainerIpAddress() +
            ":" + etcd.getMappedPort(ETCD_PORT)
    ));
  }

  private List<URI> getProxiedClientEndpoints() {
    return List.of(URI.create(
        "https://" + etcdProxy.getContainerIpAddress() +
            ":" + etcdProxy.getProxyPort()
    ));
  }

  class A implements Closeable {

    @Override
    public void close() throws IOException {
      System.out.println("Close called.");
    }
  }
  @Test
  public void testNodeJoin() throws Exception {
    Closer closer = Closer.create();
    A aref = new A();
    aref.close();
    closer.register(aref);
    aref = null;
    closer.close();
    System.out.println("test done.");
  }

  @Test
  public void testJetcd() {
    Client client = Client.builder()
        .endpoints(
        "http://localhost:2379" //, "http://etcd1:2379", "http://etcd2:2379"
        ).build();
  }

  @Test
  public void testConn() {
//    BasicConfigurator.configure();
//    System.out.println("ENDPOINTS:" + getClientEndpoints());
//    EtcdClient eClient = new EtcdClient("TestCluster", getClientEndpoints());
//    int numOfNodes = 3;
//    try {
//      for (int i=0 ; i<numOfNodes; i++) {
//        WorkerNetAddress wkrAddr = new WorkerNetAddress().setHost("worker" + (i+1)).setRpcPort(1234);
//        WorkerServiceEntity workerServiceEntity =
//            new WorkerServiceEntity(wkrAddr);
//        if (i % 2 == 0) {
//          workerServiceEntity.mState = WorkerServiceEntity.State.AUTHORIZED;
//        }
//        eClient.mServiceDiscovery.registerAndStartSync(workerServiceEntity);
////        EtcdClient.ServiceEntityContext ctx = new EtcdClient.ServiceEntityContext(String.format("worker-%d", i+1));
////        eClient.mServiceDiscovery.registerAndStartSync(ctx);
//      }
//      Map<String, ByteBuffer> liveServices = eClient.mServiceDiscovery.getAllLiveServices();
//      StringBuilder sb = new StringBuilder("Node status:\n");
//      for (Map.Entry<String, ByteBuffer> entry : liveServices.entrySet()) {
//        WorkerServiceEntity wkrEntity = new WorkerServiceEntity();
//        DataInputStream dis = new DataInputStream(new ByteBufferBackedInputStream(entry.getValue()));
//        wkrEntity.deserialize(dis);
//        sb.append(wkrEntity.mAddress.getHost() + ":"
//            + wkrEntity.mAddress.getRpcPort()
//            + "  :  " + wkrEntity.mState.toString() + "\n");
//      }
//      System.out.println(sb.toString());
//      while (true) {
//        try {
//          Thread.sleep(1000);
//        } catch (InterruptedException ex) {
//          break;
//        }
//      }
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
  }
}