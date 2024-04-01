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

import static org.junit.Assert.assertThrows;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerIdentityTestUtils;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.WorkerState;

import com.google.common.collect.Lists;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import io.etcd.jetcd.Auth;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.auth.AuthEnableResponse;
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
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MembershipManagerTest {
  private static final Network NETWORK = Network.newNetwork();
  private static final int ETCD_PORT = 2379;
  private static final String WORKER_FAILURE_TIMEOUT = "15sec";
  private static final long WORKER_FAILURE_TIMEOUT_IN_MILLIS =
      FormatUtils.parseTimeSize(WORKER_FAILURE_TIMEOUT);

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private static ToxiproxyContainer.ContainerProxy sEtcdProxy;

  //Uncomment for logging when need debugging
  /*
  @BeforeClass
  public static void init() {
    PropertyConfigurator.configure("<path_to_alluxio>/conf/log4j.properties");
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
    Configuration.set(PropertyKey.WORKER_FAILURE_DETECTION_TIMEOUT, WORKER_FAILURE_TIMEOUT);
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

  public MembershipManager getHealthyEtcdMemberMgr() {
    return new EtcdMembershipManager(Configuration.global(), getHealthyAlluxioEtcdClient());
  }

  public MembershipManager getToxicEtcdMemberMgr() {
    return new EtcdMembershipManager(Configuration.global(), getToxicAlluxioEtcdClient());
  }

  public void enableEtcdAuthentication() throws ExecutionException, InterruptedException {
    // root has full permission and is first user to enable authentication
    // so just use root user for test etcd user/password connection.
    String user = "root";
    String password = "root";

    AlluxioEtcdClient alluxioEtcdClient = getHealthyAlluxioEtcdClient();
    Auth authClient = alluxioEtcdClient.getEtcdClient().getAuthClient();
    authClient.roleAdd(ByteSequence.from("root", StandardCharsets.UTF_8));
    authClient.userAdd(
        ByteSequence.from("root", StandardCharsets.UTF_8),
        ByteSequence.from("root", StandardCharsets.UTF_8)).get();
    authClient.userGrantRole(
        ByteSequence.from("root", StandardCharsets.UTF_8),
        ByteSequence.from("root", StandardCharsets.UTF_8)).get();
    AuthEnableResponse enableResponse = authClient.authEnable().get();
  }

  public void disableEtcdAuthentication() throws ExecutionException, InterruptedException {
    // this method assumes enableEtcdAuthentication() has already been called.
    String user = "root";
    String password = "root";

    AlluxioEtcdClient alluxioEtcdClient = getHealthyAlluxioEtcdClient();
    Auth authClient = alluxioEtcdClient.getEtcdClient().getAuthClient();
    authClient.authDisable();
  }

  @Test
  public void testEtcdMembershipWithAuth() throws Exception {
    enableEtcdAuthentication();
    Configuration.set(PropertyKey.ETCD_USERNAME, "root");
    Configuration.set(PropertyKey.ETCD_PASSWORD, "root");
    testEtcdMembership(getHealthyEtcdMemberMgr());
    disableEtcdAuthentication();
    Configuration.unset(PropertyKey.ETCD_USERNAME);
    Configuration.unset(PropertyKey.ETCD_PASSWORD);
  }

  @Test
  public void testEtcdMembershipWithoutAuth() throws Exception {
    testEtcdMembership(getHealthyEtcdMemberMgr());
  }

  public void testEtcdMembership(MembershipManager membershipManager) throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker1").setContainerHost("containerhostname1")
            .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(1021));
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker2").setContainerHost("containerhostname2")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(2021));
    WorkerInfo wkr3 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker3").setContainerHost("containerhostname3")
            .setRpcPort(3000).setDataPort(3001).setWebPort(3011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(3031));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    membershipManager.join(wkr3);
    List<WorkerInfo> wkrs = new ArrayList<>();
    wkrs.add(new WorkerInfo(wkr1).setState(WorkerState.LIVE));
    wkrs.add(new WorkerInfo(wkr2).setState(WorkerState.LIVE));
    wkrs.add(new WorkerInfo(wkr3).setState(WorkerState.LIVE));
    List<WorkerInfo> allMembers = membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    Assert.assertEquals(wkrs, allMembers);

    membershipManager.stopHeartBeat(wkr2);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    CommonUtils.waitFor("Service's lease close and service key got deleted.",
        () -> {
          try {
            return !membershipManager.getFailedMembers().isEmpty();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));
    List<WorkerInfo> expectedFailedList = new ArrayList<>();
    expectedFailedList.add(new WorkerInfo(wkr2).setState(WorkerState.LOST));
    Assert.assertEquals(expectedFailedList,
        Lists.newArrayList(membershipManager.getFailedMembers()));
    List<WorkerInfo> actualLiveMembers = membershipManager.getLiveMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    List<WorkerInfo> expectedLiveMembers = new ArrayList<>();
    expectedLiveMembers.add(new WorkerInfo(wkr1).setState(WorkerState.LIVE));
    expectedLiveMembers.add(new WorkerInfo(wkr3).setState(WorkerState.LIVE));
    Assert.assertEquals(expectedLiveMembers, actualLiveMembers);
  }

  @Test
  public void testServiceRegistryMembershipManager() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.SERVICE_REGISTRY);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    AlluxioEtcdClient client = getHealthyAlluxioEtcdClient();
    ServiceRegistryMembershipManager membershipManager =
        new ServiceRegistryMembershipManager(Configuration.global(), client);
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker1").setContainerHost("containerhostname1")
            .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker2").setContainerHost("containerhostname2")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    WorkerInfo wkr3 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker3").setContainerHost("containerhostname3")
            .setRpcPort(3000).setDataPort(3001).setWebPort(3011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    membershipManager.join(wkr3);
    List<WorkerInfo> wkrs = new ArrayList<>();
    wkrs.add(new WorkerInfo(wkr1).setState(WorkerState.LIVE));
    wkrs.add(new WorkerInfo(wkr2).setState(WorkerState.LIVE));
    wkrs.add(new WorkerInfo(wkr3).setState(WorkerState.LIVE));
    List<WorkerInfo> allMembers = membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    Assert.assertEquals(wkrs, allMembers);
    List<String> strs =
        client.getChildren("/").stream().map(kv -> kv.getKey().toString(StandardCharsets.UTF_8))
            .collect(Collectors.toList());
    Assert.assertEquals(3, strs.size());
    for (String str : strs) {
      Assert.assertTrue(str.contains("/ServiceDiscovery/DefaultAlluxioCluster/worker"));
    }
    membershipManager.stopHeartBeat(wkr2);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    CommonUtils.waitFor("Service's lease close and service key got deleted.", () -> {
      try {
        return membershipManager.getLiveMembers().size() == 2;
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Unexpected error while getting failed members: %s", e));
      }
    }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));
    Assert.assertTrue(Lists.newArrayList(membershipManager.getFailedMembers()).isEmpty());
    List<WorkerInfo> actualLiveMembers = membershipManager.getLiveMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    List<WorkerInfo> expectedLiveMembers = new ArrayList<>();
    expectedLiveMembers.add(new WorkerInfo(wkr1).setState(WorkerState.LIVE));
    expectedLiveMembers.add(new WorkerInfo(wkr3).setState(WorkerState.LIVE));
    Assert.assertEquals(expectedLiveMembers, actualLiveMembers);
    Assert.assertEquals(membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList()), actualLiveMembers);
  }

  @Test
  public void testFlakyNetwork() throws Exception {
    System.out.println("WORKER_FAILURE_TIMEOUT is configured to be "
        + WORKER_FAILURE_TIMEOUT);
    MembershipManager membershipManager = getToxicEtcdMemberMgr();
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker-1").setContainerHost("containerhostname1")
            .setRpcPort(29999).setDataPort(29997).setWebPort(30000)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(30001));
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker-2").setContainerHost("containerhostname2")
            .setRpcPort(29999).setDataPort(29997).setWebPort(30000)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(30001));
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
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));

    MembershipManager healthyMgr = getHealthyEtcdMemberMgr();
    System.out.println("All Node Status:\n" + healthyMgr.showAllMembers());
    System.out.println(String.format("Induce %d sec latency upstream to etcd...",
        TimeUnit.MILLISECONDS.toSeconds(WORKER_FAILURE_TIMEOUT_IN_MILLIS / 2)));
    /* For induced latency lower than WORKER_FAILURE_TIMEOUT_IN_MILLIS, we shouldn't see
       that the worker is considered failed. */
    sEtcdProxy.toxics()
        .latency("latency", ToxicDirection.UPSTREAM, WORKER_FAILURE_TIMEOUT_IN_MILLIS / 2);
    // assert that we will never see any worker considered FAIL.
    Assert.assertThrows(TimeoutException.class, () ->
        CommonUtils.waitFor("Workers network errored but not considered fail",
            () -> {
              try {
                return !healthyMgr.getFailedMembers().isEmpty();
              } catch (IOException e) {
                throw new RuntimeException(
                    String.format("Unexpected error while getting failed members: %s", e));
              }
            }, WaitForOptions.defaults().setTimeoutMs(TimeUnit.SECONDS.toMillis(10))));
    System.out.println("All Node Status:\n" + healthyMgr.showAllMembers());
    System.out.println(String.format(
        "Remove latency toxics and induce %d sec latency upstream to etcd...",
        TimeUnit.MILLISECONDS.toSeconds(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 10000)));
    sEtcdProxy.toxics().get("latency").remove();
    sEtcdProxy.toxics()
        .latency("latency", ToxicDirection.UPSTREAM, WORKER_FAILURE_TIMEOUT_IN_MILLIS + 10000);
    CommonUtils.waitFor("Workers network errored",
        () -> {
          try {
            return !healthyMgr.getFailedMembers().isEmpty();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 10000));

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
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS));
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
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker1").setContainerHost("containerhostname1")
            .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(1021));
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker2").setContainerHost("containerhostname2")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(2021));
    WorkerInfo wkr3 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker3").setContainerHost("containerhostname3")
            .setRpcPort(3000).setDataPort(3001).setWebPort(3011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(3021));
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

  @Test
  public void testSameWorkerIdentityConflict() throws Exception {
    final MembershipManager membershipManager = getHealthyEtcdMemberMgr();
    // in non-k8s env, no two workers can assume same identity, unless
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    WorkerIdentity workerIdentity1 = WorkerIdentityTestUtils.randomUuidBasedId();
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(workerIdentity1)
        .setAddress(new WorkerNetAddress()
            .setHost("worker1").setContainerHost("containerhostname1")
            .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(1021));
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(workerIdentity1)
        .setAddress(new WorkerNetAddress()
            .setHost("worker2").setContainerHost("containerhostname2")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(2021));
    membershipManager.join(wkr1);
    // bring wrk1 down and join wrk2 with a same worker identity.
    membershipManager.stopHeartBeat(wkr1);
    CommonUtils.waitFor("wkr1 is not alive.", () -> {
      try {
        return membershipManager.getFailedMembers().getWorkerById(workerIdentity1).isPresent();
      } catch (IOException e) {
        // IGNORE
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));
    try {
      membershipManager.join(wkr2);
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof AlreadyExistsException);
    }

    // only in k8s env, it should allow same worker identity assumption.
    Configuration.set(PropertyKey.K8S_ENV_DEPLOYMENT, true);
    final MembershipManager membershipManager1 = getHealthyEtcdMemberMgr();
    membershipManager1.join(wkr2);
    // check if joined with correct info onto etcd
    Optional<WorkerInfo> curWorkerInfo = membershipManager1.getLiveMembers()
        .getWorkerById(workerIdentity1);
    Assert.assertTrue(curWorkerInfo.isPresent());
    Assert.assertEquals(wkr2.getAddress(), curWorkerInfo.get().getAddress());
  }

  @Test
  public void testOptionalHttpPortChangeInWorkerAddress() throws Exception {
    final MembershipManager membershipManager = getHealthyEtcdMemberMgr();
    Assert.assertTrue(membershipManager instanceof EtcdMembershipManager);
    // join without http server ports
    WorkerIdentity workerIdentity = WorkerIdentityTestUtils.randomUuidBasedId();
    WorkerNetAddress workerNetAddress = new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock");
    WorkerInfo wkr = new WorkerInfo()
        .setIdentity(workerIdentity)
        .setAddress(workerNetAddress);
    membershipManager.join(wkr);
    Optional<WorkerInfo> curWorkerInfo = membershipManager.getLiveMembers()
        .getWorkerById(workerIdentity);
    Assert.assertTrue(curWorkerInfo.isPresent());
    membershipManager.stopHeartBeat(wkr);
    CommonUtils.waitFor("wkr is not alive.", () -> {
      try {
        return membershipManager.getFailedMembers().getWorkerById(workerIdentity).isPresent();
      } catch (IOException e) {
        // IGNORE
        return false;
      }
    }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));

    // set the http server port and rejoin
    workerNetAddress.setHttpServerPort(1021);
    membershipManager.join(wkr);
    // check if the worker is rejoined and information updated
    WorkerClusterView allMembers = membershipManager.getAllMembers();
    Assert.assertEquals(1, allMembers.size());
    curWorkerInfo = membershipManager.getLiveMembers().getWorkerById(workerIdentity);
    Assert.assertTrue(curWorkerInfo.isPresent());
    Assert.assertEquals(wkr.getAddress(), curWorkerInfo.get().getAddress());
    Assert.assertEquals(wkr.getAddress().getHttpServerPort(),
        curWorkerInfo.get().getAddress().getHttpServerPort());
  }

  @Test
  public void testDecommission() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMBERSHIP_MANAGER_TYPE, MembershipType.ETCD);
    Configuration.set(PropertyKey.ETCD_ENDPOINTS, getClientEndpoints());
    MembershipManager membershipManager = getHealthyEtcdMemberMgr();
    WorkerInfo wkr1 = new WorkerInfo()
        .setIdentity(WorkerIdentityTestUtils.randomUuidBasedId())
        .setAddress(new WorkerNetAddress()
            .setHost("worker1").setContainerHost("containerhostname1")
            .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    WorkerIdentity wkr2Id = WorkerIdentityTestUtils.randomUuidBasedId();
    WorkerInfo wkr2 = new WorkerInfo()
        .setIdentity(wkr2Id)
        .setAddress(new WorkerNetAddress()
            .setHost("worker2").setContainerHost("containerhostname2")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    membershipManager.join(wkr1);
    membershipManager.join(wkr2);
    List<WorkerInfo> wkrs = new ArrayList<>();
    wkrs.add(new WorkerInfo(wkr1).setState(WorkerState.LIVE));
    wkrs.add(new WorkerInfo(wkr2).setState(WorkerState.LIVE));
    List<WorkerInfo> allMembers = membershipManager.getAllMembers().stream()
        .sorted(Comparator.comparing(w -> w.getAddress().getHost()))
        .collect(Collectors.toList());
    Assert.assertEquals(wkrs, allMembers);

    // try to decommission a running worker will be rejected
    assertThrows(InvalidArgumentException.class, () -> membershipManager.decommission(wkr2));

    // try stop and decommission
    membershipManager.stopHeartBeat(wkr2);
    CommonUtils.waitFor("Worker to stop",
        () -> {
          try {
            return membershipManager.getFailedMembers()
                .getWorkerById(wkr2.getIdentity()).isPresent();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting failed members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));
    membershipManager.decommission(wkr2);
    Assert.assertFalse(membershipManager.getAllMembers()
        .getWorkerById(wkr2.getIdentity()).isPresent());

    // some other worker with a same id could register again
    WorkerInfo wkr2Replacement = new WorkerInfo()
        .setIdentity(wkr2Id)
        .setAddress(new WorkerNetAddress()
            .setHost("worker3").setContainerHost("containerhostname3")
            .setRpcPort(2000).setDataPort(2001).setWebPort(2011)
            .setDomainSocketPath("/var/lib/domain.sock"));
    membershipManager.join(wkr2Replacement);
    CommonUtils.waitFor("Worker2 replacement to be up.",
        () -> {
          try {
            return membershipManager.getLiveMembers().getWorkerById(wkr2Id).isPresent();
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Unexpected error while getting live members: %s", e));
          }
        }, WaitForOptions.defaults().setTimeoutMs(WORKER_FAILURE_TIMEOUT_IN_MILLIS + 1000));
    Optional<WorkerInfo> newWkr2Entity = membershipManager.getAllMembers().getWorkerById(wkr2Id);
    Assert.assertTrue(newWkr2Entity.isPresent());
    wkr2Replacement.setState(WorkerState.LIVE);
    Assert.assertEquals(wkr2Replacement, newWkr2Entity.get());
  }
}
