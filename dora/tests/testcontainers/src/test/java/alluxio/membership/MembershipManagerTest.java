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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import eu.rekawek.toxiproxy.model.ToxicDirection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class MembershipManagerTest {
  private static final Network NETWORK = Network.newNetwork();
  private static final int ETCD_PORT = 2379;
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private static ToxiproxyContainer.ContainerProxy sEtcdProxy;

  //Uncomment for logging when need debugging
  /*
  @BeforeClass
  public static void init() {
    PropertyConfigurator.configure("alluxio/conf/log4j.properties");
    Properties props = new Properties();
    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
  }
  */

  @ClassRule
  public static final GenericContainer<?> ETCD_CONTAINER =
      new GenericContainer<>("quay.io/coreos/etcd:latest")
          .withCommand("etcd",
              "--listen-client-urls", "http://0.0.0.0:" + ETCD_PORT,
              "--advertise-client-urls", "http://0.0.0.0:" + ETCD_PORT)
          .withExposedPorts(ETCD_PORT)
          .withNetwork(NETWORK);

  @ClassRule
  public static final ToxiproxyContainer TOXIPROXY =
      new ToxiproxyContainer(
          "ghcr.io/shopify/toxiproxy:2.5.0")
          .withNetwork(NETWORK)
          .withNetworkAliases("toxiproxy");

  private static List<String> getClientEndpoints() {
    ArrayList<String> clientEps = new ArrayList<>();
    clientEps.add("https://" + ETCD_CONTAINER.getHost()
        + ":" + ETCD_CONTAINER.getMappedPort(ETCD_PORT));
    return clientEps;
  }

  private static List<URI> getProxiedClientEndpoints() {
    ArrayList<URI> clientURIs = new ArrayList<>();
    clientURIs.add(URI.create(
        "https://" + sEtcdProxy.getContainerIpAddress()
            + ":" + sEtcdProxy.getProxyPort()));
    return clientURIs;
  }

  @BeforeClass
  public static void beforeAll() throws Exception {
    sEtcdProxy = TOXIPROXY.getProxy(ETCD_CONTAINER, ETCD_PORT);
  }

  @AfterClass
  public static void afterAll() {
    NETWORK.close();
  }

  @Before
  public void before() throws IOException {
    List<String> strs = getHealthyAlluxioEtcdClient().getChildren("/")
        .stream().map(kv -> kv.getKey().toString(StandardCharsets.UTF_8))
        .collect(Collectors.toList());
    System.out.println("Before, all kvs on etcd:" + strs);
  }

  @After
  public void after() throws IOException {
    // Wipe out clean all etcd kv pairs
    getHealthyAlluxioEtcdClient().deleteForPath("/", true);
    AlluxioEtcdClient.getInstance(Configuration.global()).mServiceDiscovery.unregisterAll();
    List<String> strs = getHealthyAlluxioEtcdClient().getChildren("/")
        .stream().map(kv -> kv.getKey().toString(StandardCharsets.UTF_8))
        .collect(Collectors.toList());
    System.out.println("After, all kvs on etcd:" + strs);
  }

  @Test
  public void testEtcdMembership() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    MembershipManager membershipManager = MembershipManager.Factory.create(Configuration.global());
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    WorkerInfo wkr1 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock"));
    WorkerInfo wkr2 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker2").setContainerHost("containerhostname2")
        .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
        .setDomainSocketPath("/var/lib/domain.sock"));
    WorkerInfo wkr3 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker3").setContainerHost("containerhostname3")
        .setRpcPort(3000).setDataPort(3001).setWebPort(3011)
        .setDomainSocketPath("/var/lib/domain.sock"));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    membershipManager.join(wkr3);
    List<WorkerInfo> wkrs = new ArrayList<>();
    wkrs.add(wkr1);
    wkrs.add(wkr2);
    wkrs.add(wkr3);
    List<WorkerInfo> allMembers = membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    Assert.assertEquals(wkrs, allMembers);

    membershipManager.stopHeartBeat(wkr2);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    CommonUtils.waitFor("Service's lease close and service key got deleted.",
        () -> {
          try {
            return membershipManager.getFailedMembers().size() > 0;
          } catch (IOException e) {
            throw new RuntimeException(
            String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10)));
    List<WorkerInfo> expectedFailedList = new ArrayList<>();
    expectedFailedList.add(wkr2);
    Assert.assertEquals(expectedFailedList, membershipManager.getFailedMembers());
    List<WorkerInfo> actualLiveMembers = membershipManager.getLiveMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    List<WorkerInfo> expectedLiveMembers = new ArrayList<>();
    expectedLiveMembers.add(wkr1);
    expectedLiveMembers.add(wkr3);
    Assert.assertEquals(expectedLiveMembers, actualLiveMembers);
  }

  public AlluxioEtcdClient getHealthyAlluxioEtcdClient() {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    return new AlluxioEtcdClient(Configuration.global());
  }

  public AlluxioEtcdClient getToxicAlluxioEtcdClient() {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getProxiedClientEndpoints());
    return new AlluxioEtcdClient(Configuration.global());
  }

  public MembershipManager getHealthyEtcdMemberMgr() throws IOException {
    return new EtcdMembershipManager(Configuration.global(), getHealthyAlluxioEtcdClient());
  }

  @Test
  public void testFlakyNetwork() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getProxiedClientEndpoints());
    MembershipManager membershipManager = MembershipManager.Factory.create(Configuration.global());
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    TieredIdentity ti = TieredIdentityFactory.localIdentity(Configuration.global());
    WorkerInfo wkr1 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker-1").setContainerHost("containerhostname1")
        .setRpcPort(29999).setDataPort(29997).setWebPort(30000)
        .setDomainSocketPath("/var/lib/domain.sock").setTieredIdentity(ti));
    WorkerInfo wkr2 = new WorkerInfo().setAddress(new WorkerNetAddress()
        .setHost("worker-2").setContainerHost("containerhostname2")
        .setRpcPort(29999).setDataPort(29997).setWebPort(30000)
        .setDomainSocketPath("/var/lib/domain.sock").setTieredIdentity(ti));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    CommonUtils.waitFor("Workers joined",
        () -> {
          try {
            return !membershipManager.getLiveMembers().isEmpty();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting live members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10)));

    MembershipManager healthyMgr = getHealthyEtcdMemberMgr();
    System.out.println("All Node Status:\n" + healthyMgr.showAllMembers());
    System.out.println("Induce 10 sec latency upstream to etcd...");
    sEtcdProxy.toxics()
        .latency("latency", ToxicDirection.UPSTREAM, 10000);
    CommonUtils.waitFor("Workers network errored",
        () -> {
          try {
            return !healthyMgr.getFailedMembers().isEmpty();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10)));
    System.out.println("All Node Status:\n" + healthyMgr.showAllMembers());
    System.out.println("Remove latency toxics...");
    sEtcdProxy.toxics().get("latency").remove();
    CommonUtils.waitFor("Workers network recovered",
        () -> {
          try {
            return healthyMgr.getFailedMembers().isEmpty();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10)));
    System.out.println("All Node Status:\n" + healthyMgr.showAllMembers());
  }

  @Test
  public void testStaticMembership() throws Exception {
    File file = mFolder.newFile();
    PrintStream ps = new PrintStream(file);
    ps.println("worker1");
    ps.println("worker2");
    ps.println("worker3");
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.STATIC);
    Configuration.set(PropertyKey.WORKER_STATIC_MEMBERSHIP_MANAGER_CONFIG_FILE,
        file.getAbsolutePath());

    MembershipManager membershipManager = MembershipManager.Factory.create(Configuration.global());
    Assert.assertTrue(membershipManager instanceof StaticMembershipManager);
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
    List<String> wkrHosts = new ArrayList<>();
    wkrHosts.add(wkr1.getAddress().getHost());
    wkrHosts.add(wkr2.getAddress().getHost());
    wkrHosts.add(wkr3.getAddress().getHost());
    // As for static membership mgr, only hostnames are provided in the static file
    List<String> allMemberHosts = membershipManager.getAllMembers().stream()
        .map(w -> w.getAddress().getHost())
        .sorted()
        .collect(Collectors.toList());
    Assert.assertEquals(wkrHosts, allMemberHosts);
  }
}
