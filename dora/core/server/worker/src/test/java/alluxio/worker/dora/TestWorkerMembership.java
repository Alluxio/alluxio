package alluxio.worker.dora;


import alluxio.membership.EtcdClient;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBufInputStream;
import org.apache.log4j.BasicConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

  @Before
  public void beforeEach() {
    etcdProxy = toxiproxy.getProxy(etcd, ETCD_PORT);
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
  public void testConn() {
//    BasicConfigurator.configure();
    System.out.println("ENDPOINTS:" + getClientEndpoints());
    EtcdClient eClient = new EtcdClient("TestCluster", getClientEndpoints());
    int numOfNodes = 3;
    try {
      for (int i=0 ; i<numOfNodes; i++) {
        WorkerNetAddress wkrAddr = new WorkerNetAddress().setHost("worker" + (i+1)).setRpcPort(1234);
        PagedDoraWorker.PagedDoraWorkerServiceEntity workerServiceEntity =
            new PagedDoraWorker.PagedDoraWorkerServiceEntity(wkrAddr);
        if (i % 2 == 0) {
          workerServiceEntity.mState = PagedDoraWorker.PagedDoraWorkerServiceEntity.State.AUTHORIZED;
        }
        eClient.mServiceDiscovery.registerAndStartSync(workerServiceEntity);
//        EtcdClient.ServiceEntityContext ctx = new EtcdClient.ServiceEntityContext(String.format("worker-%d", i+1));
//        eClient.mServiceDiscovery.registerAndStartSync(ctx);
      }
      Map<String, ByteBuffer> liveServices = eClient.mServiceDiscovery.getAllLiveServices();
      StringBuilder sb = new StringBuilder("Node status:\n");
      for (Map.Entry<String, ByteBuffer> entry : liveServices.entrySet()) {
        PagedDoraWorker.PagedDoraWorkerServiceEntity wkrEntity = new PagedDoraWorker.PagedDoraWorkerServiceEntity();
        DataInputStream dis = new DataInputStream(new ByteBufferBackedInputStream(entry.getValue()));
        wkrEntity.deserialize(dis);
        sb.append(wkrEntity.mAddress.getHost() + ":"
            + wkrEntity.mAddress.getRpcPort()
            + "  :  " + wkrEntity.mState.toString() + "\n");
      }
      System.out.println(sb.toString());
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          break;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }
}