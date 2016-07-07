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

package alluxio.yarn;

import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.SystemPropertyRule;
import alluxio.util.CommonUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Unit tests for {@link ApplicationMaster}.
 */
// TODO(andrew): Add tests for failure cases
public class ApplicationMasterTest {
  private static final String MASTER_ADDRESS = "localhost";
  private static final int NUM_WORKERS = 25;
  private static final int MASTER_MEM_MB =
      (int) Configuration.getBytes(Constants.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
  private static final int MASTER_CPU =
      Configuration.getInt(Constants.INTEGRATION_MASTER_RESOURCE_CPU);
  private static final int WORKER_MEM_MB =
      (int) Configuration.getBytes(Constants.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;
  private static final int RAMDISK_MEM_MB =
      (int) Configuration.getBytes(Constants.WORKER_MEMORY_SIZE) / Constants.MB;
  private static final int WORKER_CPU =
      Configuration.getInt(Constants.INTEGRATION_WORKER_RESOURCE_CPU);

  private ApplicationMaster mMaster;
  private ApplicationMasterPrivateAccess mPrivateAccess;
  private NMClient mNMClient;
  private AMRMClientAsync<ContainerRequest> mRMClient;
  private YarnClient mYarnClient;

  // This is needed for when the ApplicationMaster calls hadoop's FileSystem.get().
  @ClassRule
  public static SystemPropertyRule sSystemPropertyRule =
      new SystemPropertyRule("HADOOP_USER_NAME", "testuser");

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    setupApplicationMaster(ImmutableMap.<String, String>of());
  }

  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  private void setupApplicationMaster(Map<String, String> properties) throws Exception {
    for (Entry<String, String> entry : properties.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }

    URI resourceUri = mTemporaryFolder.newFolder().toURI();
    String resourcePath = resourceUri.getPath();
    FileUtils.createFile(PathUtils.concatPath(resourcePath, YarnUtils.ALLUXIO_TARBALL));
    FileUtils.createFile(PathUtils.concatPath(resourcePath, YarnUtils.ALLUXIO_SETUP_SCRIPT));

    // Mock Yarn client
    mYarnClient = (YarnClient) Mockito.mock(YarnClient.class);

    // Mock Node Manager client
    mNMClient = Mockito.mock(NMClient.class);

    // Mock Application Master Resource Manager client
    @SuppressWarnings("unchecked")
    AMRMClientAsync<ContainerRequest> amrm =
        (AMRMClientAsync<ContainerRequest>) Mockito.mock(AMRMClientAsync.class);
    mRMClient = amrm;

    mMaster = new ApplicationMaster(NUM_WORKERS, MASTER_ADDRESS, resourceUri.toString(),
        mYarnClient, mNMClient);
    Whitebox.setInternalState(mMaster, "mRMClient", mRMClient);
    mPrivateAccess = new ApplicationMasterPrivateAccess(mMaster);

    mMaster.start();
  }

  /**
   * Tests that start() properly registers the application master.
   */
  @Test
  public void startTest() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName();
    Mockito.verify(mRMClient).registerApplicationMaster(hostname, 0, "");
  }

  /**
   * Tests that the correct type and number of containers are requested.
   */
  @Test(timeout = 10000)
  public void requestContainersOnceTest() throws Exception {
    // Mock the Yarn client to give a NodeReport with NUM_WORKERS nodes
    List<NodeReport> nodeReports = Lists.newArrayList();
    final List<String> nodeHosts = Lists.newArrayList();
    for (int i = 0; i < NUM_WORKERS; i++) {
      String host = "host" + i;
      NodeReport report = Mockito.mock(NodeReport.class);
      Mockito.when(report.getNodeId()).thenReturn(NodeId.newInstance(host, 0));
      nodeReports.add(report);
      nodeHosts.add(host);
    }
    // We need to use anyVararg because Mockito is dumb and assumes that an array argument must be
    // vararg. Using regular any() will only match an array if it has length 1.
    Mockito.when(mYarnClient.getNodeReports(Matchers.<NodeState[]>anyVararg()))
        .thenReturn(nodeReports);

    // Mock the Resource Manager to "allocate" containers when they are requested and update
    // ApplicationMaster internal state
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        mPrivateAccess.getMasterAllocated().countDown();
        return null;
      }
    }).when(mRMClient).addContainerRequest(Mockito.argThat(getMasterContainerMatcher()));
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        Multiset<String> workerNodes = mPrivateAccess.getWorkerHosts();
        synchronized (workerNodes) {
          workerNodes.add("host-" + UUID.randomUUID());
          mPrivateAccess.getOutstandingWorkerContainerReqeustsLatch().countDown();
          workerNodes.notify();
          if (workerNodes.size() == NUM_WORKERS) {
            // Once all workers are allocated, we shut down the master so that
            // requestContainers() doesn't run forever
            mMaster.onShutdownRequest();
          }
        }
        return null;
      }
    }).when(mRMClient).addContainerRequest(Mockito.argThat(getWorkerContainerMatcher(nodeHosts)));

    // This will hang if incorrect worker container requests are made
    mMaster.requestContainers();

    // Verify that the right types and numbers of containers were requested
    Mockito.verify(mRMClient).addContainerRequest(Mockito.argThat(getMasterContainerMatcher()));
    Mockito.verify(mRMClient, Mockito.times(NUM_WORKERS))
        .addContainerRequest(Mockito.argThat(getWorkerContainerMatcher(nodeHosts)));
  }

  /**
   * Tests that the application master will reject and re-request worker containers whose hosts are
   * already used by other workers. This tests {@link ApplicationMaster} as a whole, only mocking
   * its clients.
   */
  @Test(timeout = 10000)
  public void negotiateUniqueWorkerHostsTest() throws Exception {
    mockResourceManager(NUM_WORKERS);

    // Wait for all workers to be allocated, then shut down mMaster
    getWaitForShutdownThread().start();

    mMaster.requestContainers();
    Assert.assertEquals(NUM_WORKERS, mPrivateAccess.getWorkerHosts().size());
  }

  /**
   * Tests that the application master will reject and re-request worker containers whose hosts are
   * already used by other workers. This tests {@link ApplicationMaster} as a whole, only mocking
   * its clients.
   */
  @Test(timeout = 10000)
  public void spreadWorkersEvenlyOverHostsTest() throws Exception {
    int workersPerHost = 5;
    Assert.assertEquals("NUM_WORKERS should be a multiple of workersPerHost", 0,
        NUM_WORKERS % workersPerHost);
    setupApplicationMaster(ImmutableMap.of(
        Constants.INTEGRATION_YARN_WORKERS_PER_HOST_MAX, Integer.toString(workersPerHost)));

    mockResourceManager(NUM_WORKERS / workersPerHost);

    // Wait for all workers to be allocated, then shut down mMaster
    getWaitForShutdownThread().start();

    mMaster.requestContainers();
    for (String host : mPrivateAccess.getWorkerHosts()) {
      Assert.assertEquals(workersPerHost, mPrivateAccess.getWorkerHosts().count(host));
    }
    Assert.assertEquals(NUM_WORKERS, mPrivateAccess.getWorkerHosts().size());
  }

  /**
   * @return a {@link Thread} which will wait until mMaster has NUM_WORKERS workers launched, then
   *         call mMaster.onShutdownRequest()
   */
  private Thread getWaitForShutdownThread() {
    return new Thread(new Runnable() {
      @Override
      public void run() {
        while (mPrivateAccess.getWorkerHostsSize() < NUM_WORKERS) {
          CommonUtils.sleepMs(30);
        }
        mMaster.onShutdownRequest();
      }
    });
  }

  /**
   * Mocks mRMClient to randomly allocated one of the requested hosts.
   *
   * This involves
   * 1) Creating NUM_WORKERS mock containers, each with a different mock host
   * 2) Mocking mYarnClient to return the mock hosts of the mock containers
   * 3) Mocking mRMClient.addContainerRequest to asynchronously call mMaster.onContainersAllocated
   * with a random container on a requested host
   *
   * @param numContainers the number of mock container hosts
   */
  private void mockResourceManager(int numContainers) throws Exception {
    final Random random = new Random();
    final List<Container> mockContainers = Lists.newArrayList();
    List<NodeReport> nodeReports = Lists.newArrayList();
    List<String> hosts = Lists.newArrayList(MASTER_ADDRESS);
    for (int i = 0; i < numContainers - 1; i++) {
      String host = "host" + i;
      hosts.add(host);
    }
    for (String host : hosts) {
      Container mockContainer = Mockito.mock(Container.class);
      Mockito.when(mockContainer.getNodeHttpAddress()).thenReturn(host + ":8042");
      Mockito.when(mockContainer.getNodeId()).thenReturn(NodeId.newInstance(host, 0));
      mockContainers.add(mockContainer);
      NodeReport report = Mockito.mock(NodeReport.class);
      Mockito.when(report.getNodeId()).thenReturn(NodeId.newInstance(host, 0));
      nodeReports.add(report);
    }
    // We need to use anyVararg because Mockito is dumb and assumes that an array argument must be
    // vararg. Using regular any() will only match an array if it has length 1.
    Mockito.when(mYarnClient.getNodeReports(Matchers.<NodeState[]>anyVararg()))
        .thenReturn(nodeReports);

    // Pretend to be the Resource Manager, allocating containers when they are requested.
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(final InvocationOnMock invocation) {
        new Thread(new Runnable() {
          @Override
          public void run() {
            // Allow the requests to interleave randomly
            CommonUtils.sleepMs(50 + random.nextInt(200));
            // Allocate a randomly chosen container from among the requested hosts
            ContainerRequest request = invocation.getArgumentAt(0, ContainerRequest.class);
            Set<String> requestedHosts = Sets.newHashSet(request.getNodes());
            List<Container> requestedContainers = Lists.newArrayList();
            for (Container container : mockContainers) {
              if (requestedHosts.contains(container.getNodeId().getHost())) {
                requestedContainers.add(container);
              }
            }
            mMaster.onContainersAllocated(Lists
                .newArrayList(requestedContainers.get(random.nextInt(requestedContainers.size()))));
          }
        }).start();
        return null;
      }
    }).when(mRMClient).addContainerRequest(Mockito.<ContainerRequest>any());
  }

  /**
   * Tests our advanced implementation of getProgress().
   */
  @Test
  public void progressTest() {
    Assert.assertEquals(0, mMaster.getProgress(), 0);
  }

  /**
   * Tests methods which are expected to do nothing.
   */
  @Test
  public void noOpTest() {
    mMaster.onNodesUpdated(new ArrayList<NodeReport>());
    mMaster.onError(new RuntimeException("An error occurred"));
    mMaster.onContainersCompleted(new ArrayList<ContainerStatus>());
  }

  /**
   * Tests that stop unregisters the Application Master and stops the clients.
   */
  @Test
  public void stopTest() throws Exception {
    mMaster.stop();
    Mockito.verify(mRMClient).unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    Mockito.verify(mRMClient).stop();
    Mockito.verify(mYarnClient).stop();
  }

  /**
   * Tests that the Alluxio master container is launched properly.
   */
  @Test
  public void launchAlluxioMasterContainersTest() throws Exception {
    Container mockContainer = Mockito.mock(Container.class);
    Mockito.when(mockContainer.getNodeHttpAddress()).thenReturn("1.2.3.4:8042");

    mMaster.onContainersAllocated(Lists.newArrayList(mockContainer));

    // Generate the context that we expect Yarn to launch master with.
    Map<String, String> expectedMasterEnvironment =
        ImmutableMap.<String, String>builder()
            .put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$())
            .build();
    String expectedMasterCommand =
        "./alluxio-yarn-setup.sh alluxio-master 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr ";
    ContainerLaunchContext expectedMasterContext =
        ContainerLaunchContext.newInstance(getExpectedLocalResources(), expectedMasterEnvironment,
            Lists.newArrayList(expectedMasterCommand), null, null, null);

    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer),
        Mockito.argThat(getContextMatcher(expectedMasterContext)));
    Assert.assertEquals("1.2.3.4", mPrivateAccess.getMasterContainerNetAddress());
    Assert.assertTrue(mPrivateAccess.getMasterAllocated().getCount() == 0);
  }

  /**
   * Tests that the Alluxio worker containers are launched properly.
   */
  @Test
  public void launchAlluxioWorkerContainersTest() throws Exception {
    Container mockContainer1 = Mockito.mock(Container.class);
    Container mockContainer2 = Mockito.mock(Container.class);
    // The containers must be from different hosts because we don't support multiple clients on the
    // same host.
    Mockito.when(mockContainer1.getNodeId()).thenReturn(NodeId.newInstance("host1", 0));
    Mockito.when(mockContainer2.getNodeId()).thenReturn(NodeId.newInstance("host2", 0));
    // Say that the master is allocated so that container offers are assumed to be worker offers
    mPrivateAccess.getMasterAllocated().countDown();
    mPrivateAccess.setMasterContainerAddress("masterAddress");
    mPrivateAccess.setOutstandingWorkerContainerRequestsLatch(new CountDownLatch(2));

    List<Container> containers = Lists.newArrayList(mockContainer1, mockContainer2);

    mMaster.onContainersAllocated(containers);

    // Generate the context that we expect Yarn to launch workers with.
    Map<String, String> expectedWorkerEnvironment =
        ImmutableMap.<String, String>builder()
            .put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$())
            .put("ALLUXIO_MASTER_HOSTNAME", "masterAddress")
            .put("ALLUXIO_WORKER_MEMORY_SIZE", Integer.toString(RAMDISK_MEM_MB) + ".00MB")
            .build();
    String expectedWorkerCommand =
        "./alluxio-yarn-setup.sh alluxio-worker 1><LOG_DIR>/stdout 2><LOG_DIR>/stderr ";
    ContainerLaunchContext expectedWorkerContext =
        ContainerLaunchContext.newInstance(getExpectedLocalResources(), expectedWorkerEnvironment,
            Lists.newArrayList(expectedWorkerCommand), null, null, null);

    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer1),
        Mockito.argThat(getContextMatcher(expectedWorkerContext)));
    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer2),
        Mockito.argThat(getContextMatcher(expectedWorkerContext)));
    Assert.assertEquals(containers.size(), mPrivateAccess.getWorkerHosts().size());
  }

  private static Map<String, LocalResource> getExpectedLocalResources() {
    Map<String, LocalResource> resources = Maps.newHashMap();
    resources.put(YarnUtils.ALLUXIO_TARBALL, Records.newRecord(LocalResource.class));
    resources.put(YarnUtils.ALLUXIO_SETUP_SCRIPT, Records.newRecord(LocalResource.class));
    return resources;
  }

  /**
   * Tests that large container request sizes are handled correctly.
   */
  @Test
  public void bigContainerRequestTest() {
    Configuration.set(Constants.INTEGRATION_MASTER_RESOURCE_MEM, "128gb");
    Configuration.set(Constants.INTEGRATION_WORKER_RESOURCE_MEM, "64gb");
    Configuration.set(Constants.WORKER_MEMORY_SIZE, "256gb");
    ApplicationMaster master =
        new ApplicationMaster(1, "localhost", "resourcePath", mYarnClient, mNMClient);
    Assert.assertEquals(128 * 1024, Whitebox.getInternalState(master, "mMasterMemInMB"));
    Assert.assertEquals(64 * 1024, Whitebox.getInternalState(master, "mWorkerMemInMB"));
    Assert.assertEquals(256 * 1024, Whitebox.getInternalState(master, "mRamdiskMemInMB"));
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Returns an argument matcher which matches the expected worker container request for the
   * specified hosts.
   *
   * @param hosts the hosts in the container request
   * @return the argument matcher
   */
  private ArgumentMatcher<ContainerRequest> getWorkerContainerMatcher(final List<String> hosts) {
    return new ArgumentMatcher<ContainerRequest>() {
      public boolean matches(Object arg) {
        Assert.assertTrue(arg instanceof ContainerRequest);
        ContainerRequest argContainer = (ContainerRequest) arg;
        // Wrap hosts with Sets to ignore ordering
        return argContainer.getCapability()
            .equals(Resource.newInstance(WORKER_MEM_MB + RAMDISK_MEM_MB, WORKER_CPU))
            && Sets.newHashSet(argContainer.getNodes()).equals(Sets.newHashSet(hosts))
            && argContainer.getRacks() == null
            && argContainer.getPriority().equals(Priority.newInstance(1))
            && !argContainer.getRelaxLocality();
      }
    };
  }

  /**
   * Returns an argument matcher which matches the expected master container request.
   *
   * @return the argument matcher
   */
  private ArgumentMatcher<ContainerRequest> getMasterContainerMatcher() {
    return new ArgumentMatcher<ContainerRequest>() {
      public boolean matches(Object arg) {
        boolean requireLocality = MASTER_ADDRESS.equals("localhost");
        ContainerRequest expectedWorkerContainerRequest =
            new ContainerRequest(Resource.newInstance(MASTER_MEM_MB, MASTER_CPU),
                new String[] {MASTER_ADDRESS}, null, Priority.newInstance(0), requireLocality);
        return EqualsBuilder.reflectionEquals(arg, expectedWorkerContainerRequest);
      }
    };
  }

  /**
   * @param expectedContext the context to test for matching
   * @return an argument matcher which tests for matching the given container launch context
   */
  private ArgumentMatcher<ContainerLaunchContext> getContextMatcher(
      final ContainerLaunchContext expectedContext) {
    return new ArgumentMatcher<ContainerLaunchContext>() {
      public boolean matches(Object arg) {
        if (!(arg instanceof ContainerLaunchContext)) {
          return false;
        }
        ContainerLaunchContext ctx = (ContainerLaunchContext) arg;
        // Compare only keys for local resources because values include timestamps.
        return ctx.getLocalResources().keySet().equals(expectedContext.getLocalResources().keySet())
            && ctx.getCommands().equals(expectedContext.getCommands())
            && ctx.getEnvironment().equals(expectedContext.getEnvironment());
      }
    };
  }

  private static final class ApplicationMasterPrivateAccess {
    private final ApplicationMaster mMaster;

    private ApplicationMasterPrivateAccess(ApplicationMaster master) {
      mMaster = master;
    }

    public CountDownLatch getOutstandingWorkerContainerReqeustsLatch() {
      return Whitebox.getInternalState(mMaster, "mOutstandingWorkerContainerRequestsLatch");
    }

    public void setOutstandingWorkerContainerRequestsLatch(CountDownLatch value) {
      Whitebox.setInternalState(mMaster, "mOutstandingWorkerContainerRequestsLatch", value);
    }

    public void setMasterContainerAddress(String address) {
      Whitebox.setInternalState(mMaster, "mMasterContainerNetAddress", address);
    }

    public CountDownLatch getMasterAllocated() {
      return Whitebox.getInternalState(mMaster, "mMasterContainerAllocatedLatch");
    }

    public Multiset<String> getWorkerHosts() {
      return Whitebox.getInternalState(mMaster, "mWorkerHosts");
    }

    public int getWorkerHostsSize() {
      Multiset<String> workerHosts = getWorkerHosts();
      synchronized (workerHosts) {
        return workerHosts.size();
      }
    }

    public String getMasterContainerNetAddress() {
      return Whitebox.getInternalState(mMaster, "mMasterContainerNetAddress");
    }
  }
}
