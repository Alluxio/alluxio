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

package alluxio.server.membership;

import alluxio.MembershipType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.membership.AlluxioEtcdClient;
import alluxio.membership.EtcdMembershipManager;
import alluxio.membership.MembershipManager;
import alluxio.membership.StaticMembershipManager;
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
  private static final Network network = Network.newNetwork();
  private static final int ETCD_PORT = 2379;
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private static ToxiproxyContainer.ContainerProxy etcdProxy;

  //Add for logging for debugging purpose
//  @BeforeClass
//  public static void init() {
//    PropertyConfigurator.configure("/Users/lucyge/Documents/github/alluxio/conf/log4j.properties");
//    Properties props = new Properties();
//    props.setProperty(PropertyKey.LOGGER_TYPE.toString(), "Console");
//  }

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
          "ghcr.io/shopify/toxiproxy:2.5.0")
          .withNetwork(network)
          .withNetworkAliases("toxiproxy");

  private static List<String> getClientEndpoints() {
    ArrayList<String> clientEps = new ArrayList<>();
    clientEps.add("https://" + etcd.getHost() +
        ":" + etcd.getMappedPort(ETCD_PORT));
    return clientEps;
  }

  private static List<URI> getProxiedClientEndpoints() {
    ArrayList<URI> clientURIs = new ArrayList<>();
    clientURIs.add(URI.create(
        "https://" + etcdProxy.getContainerIpAddress() +
            ":" + etcdProxy.getProxyPort()));
    return clientURIs;
  }

  @BeforeClass
  public static void beforeAll() throws Exception {
    etcdProxy = toxiproxy.getProxy(etcd, ETCD_PORT);
  }

  @AfterClass
  public static void afterAll() {
    network.close();
  }

  @Before
  public void before() {
    try {
      List<String> strs = getHealthyAlluxioEtcdClient().getChildren("/")
          .stream().map(kv -> kv.getKey().toString(StandardCharsets.UTF_8))
          .collect(Collectors.toList());
      System.out.println("Before, all kvs on etcd:" + strs);
    } catch (IOException ex) {
      // IGNORE
    }
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
    Assert.assertEquals(membershipManager.getFailedMembers(), expectedFailedList);
    List<WorkerInfo> actualLiveMembers = membershipManager.getLiveMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    List<WorkerInfo> expectedLiveMembers = new ArrayList<>();
    expectedLiveMembers.add(wkr1);
    expectedLiveMembers.add(wkr3);
    Assert.assertEquals(expectedLiveMembers, actualLiveMembers);
  }

  public AlluxioEtcdClient getHealthyAlluxioEtcdClient() {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    return new AlluxioEtcdClient(Configuration.global());
  }

  public AlluxioEtcdClient getToxicAlluxioEtcdClient() {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getProxiedClientEndpoints());
    return new AlluxioEtcdClient(Configuration.global());
  }

  public MembershipManager getHealthyEtcdMemberMgr() throws IOException {
    return new EtcdMembershipManager(Configuration.global(), getHealthyAlluxioEtcdClient());
  }

  @Test
  public void testFlakyNetwork() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.ETCD);
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
    etcdProxy.toxics()
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
    etcdProxy.toxics().get("latency").remove();
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
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_TYPE, MembershipType.STATIC);
    Configuration.set(PropertyKey.WORKER_MEMBER_STATIC_CONFIG_FILE, file.getAbsolutePath());

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
    Assert.assertEquals(allMemberHosts, wkrHosts);
  }
}
