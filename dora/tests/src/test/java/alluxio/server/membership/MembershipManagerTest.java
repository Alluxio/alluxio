package alluxio.server.membership;

import alluxio.MembershipType;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.membership.AlluxioEtcdClient;
import alluxio.membership.EtcdMembershipManager;
import alluxio.membership.MembershipManager;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.Streams;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MembershipManagerTest {
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

  @Before
  public void before() throws Exception {
    etcdProxy = toxiproxy.getProxy(etcd, ETCD_PORT);
  }


//  @BeforeClass
//  public static void init() {
//    PropertyConfigurator.configure("/Users/lucyge/Documents/github/alluxio/conf/log4j.properties");
//    Properties props = new Properties();
//    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
//  }

  @Test
  public void testEtcdMembership() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    MembershipManager membershipManager = MembershipManager.Factory.create(Configuration.global());
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    TieredIdentity ti = TieredIdentityFactory.localIdentity(Configuration.global());
    WorkerInfo wkr1 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock").setTieredIdentity(ti));
    WorkerInfo wkr2 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker2").setContainerHost("containerhostname2")
        .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
        .setDomainSocketPath("/var/lib/domain.sock").setTieredIdentity(ti));
    WorkerInfo wkr3 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker3").setContainerHost("containerhostname3")
        .setRpcPort(3000).setDataPort(3001).setWebPort(3011)
        .setDomainSocketPath("/var/lib/domain.sock").setTieredIdentity(ti));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    membershipManager.join(wkr3);
    List<WorkerInfo> wkrs = new ArrayList<>();
    wkrs.add(wkr1); wkrs.add(wkr2); wkrs.add(wkr3);
    List<WorkerInfo> allMembers = membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    Assert.assertEquals(allMembers, wkrs);

    membershipManager.stopHeartBeat(wkr2);
    CommonUtils.waitFor("Service's lease close and service key got deleted.",
        () -> {
          try {
            return membershipManager.getFailedMembers().size() > 0;
          } catch (IOException e) {
            throw new RuntimeException(
            String.format("Unexpected error while getting backup status: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10)));
    List<WorkerInfo> expectedFailedList = new ArrayList<>();
    expectedFailedList.add(wkr2);
    Assert.assertEquals(membershipManager.getFailedMembers(), expectedFailedList);
    List<WorkerInfo> actualLiveMembers = membershipManager.getLiveMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    List<WorkerInfo> expectedLiveMembers = new ArrayList<>();
    expectedLiveMembers.add(wkr1);
    expectedLiveMembers.add(wkr3);
    Assert.assertEquals(expectedLiveMembers, actualLiveMembers);
  }

  @Test
  public void testStaticMembership() throws IOException, InterruptedException, TimeoutException {

  }

}
