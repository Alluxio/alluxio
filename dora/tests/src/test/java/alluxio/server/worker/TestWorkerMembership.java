package alluxio.server.worker;


import alluxio.Constants;
import alluxio.MembershipType;
import alluxio.client.WriteType;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.LocalAlluxioCluster;
import alluxio.membership.MembershipManager;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.worker.dora.PagedDoraWorker;
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

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource;
  public LocalAlluxioCluster mLocalAlluxioCluster;
  public FileSystem mFileSystem;

  public TestWorkerMembership() throws IOException {
    int numWorkers = 1;
    mLocalAlluxioClusterResource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "10ms")
        .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "10ms")
        .setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "200ms")
//        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, SIZE_BYTES)
        .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS, Long.MAX_VALUE)
        .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
//        .setProperty(PropertyKey.USER_FILE_RESERVED_BYTES, SIZE_BYTES / 2)
        .setProperty(PropertyKey.CONF_DYNAMIC_UPDATE_ENABLED, true)
        .setProperty(PropertyKey.DORA_CLIENT_READ_LOCATION_POLICY_ENABLED, true)
        .setProperty(PropertyKey.WORKER_BLOCK_STORE_TYPE, "PAGE")
        .setProperty(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, Constants.KB)
        .setProperty(PropertyKey.WORKER_PAGE_STORE_SIZES, "1GB")
        .setProperty(PropertyKey.MASTER_WORKER_REGISTER_LEASE_ENABLED, false)
        .setNumWorkers(numWorkers)
        .setStartCluster(false)
        .build();
  }

  @Before
  public void before() throws Exception {
    mLocalAlluxioClusterResource
        .setProperty(PropertyKey.DORA_CLIENT_UFS_ROOT, mTestFolder.getRoot().getAbsolutePath())
        .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS,
            mTestFolder.getRoot().getAbsolutePath())
        .setProperty(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints())
        .setProperty(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.ETCD.name());
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    etcdProxy = toxiproxy.getProxy(etcd, ETCD_PORT);
  }

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


  private List<String> getClientEndpoints() {
    return List.of("https://" + etcd.getHost() +
        ":" + etcd.getMappedPort(ETCD_PORT));
  }

  private List<URI> getProxiedClientEndpoints() {
    return List.of(URI.create(
        "https://" + etcdProxy.getContainerIpAddress() +
            ":" + etcdProxy.getProxyPort()
    ));
  }

  @Test
  public void testStartup() throws IOException {
    FileSystemContext ctx = FileSystemContext.create();
    List<BlockWorkerInfo> workers = ctx.getCachedWorkers();
    System.out.println(workers);
  }


  @Test
  public void testJetcd() {
//    Client client = Client.builder().endpoints(getClientEndpoints()).build();

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