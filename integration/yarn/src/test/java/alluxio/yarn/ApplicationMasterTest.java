/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.yarn;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.yarn.YarnUtils.YarnContainerType;

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
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

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
@RunWith(PowerMockRunner.class)
@PrepareForTest({AMRMClientAsync.class, ApplicationMaster.class, NMClient.class, YarnUtils.class,
    YarnClient.class})
public class ApplicationMasterTest {
  private static final String MASTER_ADDRESS = "localhost";
  private static final String RESOURCE_ADDRESS = "/tmp/resource";
  private static final Configuration CONF = new Configuration();
  private static final int NUM_WORKERS = 25;
  private static final int MASTER_MEM_MB =
      (int) CONF.getBytes(Constants.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
  private static final int MASTER_CPU = CONF.getInt(Constants.INTEGRATION_MASTER_RESOURCE_CPU);
  private static final int WORKER_MEM_MB =
      (int) CONF.getBytes(Constants.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;
  private static final int RAMDISK_MEM_MB =
      (int) CONF.getBytes(Constants.WORKER_MEMORY_SIZE) / Constants.MB;
  private static final int WORKER_CPU = CONF.getInt(Constants.INTEGRATION_WORKER_RESOURCE_CPU);
  private static final Map<String, LocalResource> EXPECTED_LOCAL_RESOURCES = Maps.newHashMap();

  static {
    EXPECTED_LOCAL_RESOURCES.put(
        (String) Whitebox.getInternalState(ApplicationMaster.class, "ALLUXIO_TARBALL"),
        Records.newRecord(LocalResource.class));
    EXPECTED_LOCAL_RESOURCES.put(YarnUtils.ALLUXIO_SETUP_SCRIPT,
        Records.newRecord(LocalResource.class));
  }

  private static final String ENV_CLASSPATH =
      Whitebox.getInternalState(ApplicationMaster.class, "ENV_CLASSPATH");
  private static final String EXPECTED_WORKER_COMMAND = "test-worker-command";
  private static final Map<String, String> EXPECTED_WORKER_ENVIRONMENT =
      ImmutableMap.<String, String>builder()
          .put("CLASSPATH", ENV_CLASSPATH)
          .put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$())
          .put("ALLUXIO_MASTER_ADDRESS", "masterAddress")
          .put("ALLUXIO_WORKER_MEMORY_SIZE", Integer.toString(RAMDISK_MEM_MB) + ".00MB")
          .build();
  private static final ContainerLaunchContext EXPECTED_WORKER_CONTEXT =
      ContainerLaunchContext.newInstance(EXPECTED_LOCAL_RESOURCES, EXPECTED_WORKER_ENVIRONMENT,
          Lists.newArrayList(EXPECTED_WORKER_COMMAND), null, null, null);
  private static final String EXPECTED_MASTER_COMMAND = "test-master-command";
  private static final Map<String, String> EXPECTED_MASTER_ENVIRONMENT =
      ImmutableMap.<String, String>builder()
          .put("CLASSPATH", ENV_CLASSPATH)
          .put("ALLUXIO_HOME", ApplicationConstants.Environment.PWD.$())
          .build();
  private static final ContainerLaunchContext EXPECTED_MASTER_CONTEXT =
      ContainerLaunchContext.newInstance(EXPECTED_LOCAL_RESOURCES, EXPECTED_MASTER_ENVIRONMENT,
          Lists.newArrayList(EXPECTED_MASTER_COMMAND), null, null, null);

  private ApplicationMaster mMaster;
  private ApplicationMasterPrivateAccess mPrivateAccess;
  private NMClient mNMClient;
  private AMRMClientAsync<ContainerRequest> mRMClient;
  private YarnClient mYarnClient;

  @Before
  public void before() throws Exception {
    setupApplicationMaster(ImmutableMap.<String, String>of());
  }

  private void setupApplicationMaster(Map<String, String> properties) throws Exception {
    Configuration conf = new Configuration();
    for (Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
    mMaster = new ApplicationMaster(NUM_WORKERS, MASTER_ADDRESS, RESOURCE_ADDRESS, conf);
    mPrivateAccess = new ApplicationMasterPrivateAccess(mMaster);

    // Mock Node Manager client
    PowerMockito.mockStatic(NMClient.class);
    mNMClient = Mockito.mock(NMClient.class);
    Mockito.when(NMClient.createNMClient()).thenReturn(mNMClient);

    // Mock Application Master Resource Manager client
    PowerMockito.mockStatic(AMRMClientAsync.class);
    @SuppressWarnings("unchecked")
    AMRMClientAsync<ContainerRequest> amrm =
        (AMRMClientAsync<ContainerRequest>) Mockito.mock(AMRMClientAsync.class);
    mRMClient = amrm;
    Mockito.when(AMRMClientAsync.createAMRMClientAsync(100, mMaster)).thenReturn(mRMClient);

    // Mock Yarn client
    PowerMockito.mockStatic(YarnClient.class);
    mYarnClient = (YarnClient) Mockito.mock(YarnClient.class);
    Mockito.when(YarnClient.createYarnClient()).thenReturn(mYarnClient);

    // Partially mock Utils to avoid hdfs IO
    PowerMockito.mockStatic(YarnUtils.class);
    Mockito.when(
        YarnUtils.createLocalResourceOfFile(Mockito.<YarnConfiguration>any(), Mockito.anyString()))
        .thenReturn(Records.newRecord(LocalResource.class));
    Mockito.when(YarnUtils.buildCommand(YarnContainerType.ALLUXIO_MASTER))
        .thenReturn(EXPECTED_MASTER_COMMAND);
    Mockito.when(YarnUtils.buildCommand(YarnContainerType.ALLUXIO_WORKER))
        .thenReturn(EXPECTED_WORKER_COMMAND);

    mMaster.start();
  }

  /**
   * Tests that start() properly registers the application master.
   */
  @Test
  public void startTest() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(new Configuration());
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
    for (int i = 0; i < NUM_WORKERS; i ++) {
      String host = "host" + i;
      NodeReport report = Mockito.mock(NodeReport.class);
      Mockito.when(report.getNodeId()).thenReturn(NodeId.newInstance(host, 0));
      nodeReports.add(report);
      nodeHosts.add(host);
    }
    Mockito.when(YarnUtils.getNodeHosts(mYarnClient)).thenReturn(Sets.newHashSet(nodeHosts));

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
  @Test(timeout = 100000)
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
    for (int i = 0; i < numContainers - 1; i ++) {
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
    Mockito.when(YarnUtils.getNodeHosts(mYarnClient)).thenReturn(Sets.newHashSet(hosts));

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

    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer),
        Mockito.argThat(getContextMatcher(EXPECTED_MASTER_CONTEXT)));
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
    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer1),
        Mockito.argThat(getContextMatcher(EXPECTED_WORKER_CONTEXT)));
    Mockito.verify(mNMClient).startContainer(Mockito.same(mockContainer2),
        Mockito.argThat(getContextMatcher(EXPECTED_WORKER_CONTEXT)));
    Assert.assertEquals(containers.size(), mPrivateAccess.getWorkerHosts().size());
  }

  /**
   * Tests that the main method properly reads args and orchestrates negotiation.
   */
  @Test
  public void mainTest() throws Exception {
    ApplicationMaster mockMaster = Mockito.mock(ApplicationMaster.class);
    PowerMockito.whenNew(ApplicationMaster.class)
        .withArguments(Mockito.anyInt(), Mockito.anyString(), Mockito.anyString())
        .thenReturn(mockMaster);

    ApplicationMaster.main(new String[] {"-num_workers", "3",
        "-master_address", "address", "-resource_path", "path"});
    PowerMockito.verifyNew(ApplicationMaster.class).withArguments(3, "address", "path");

    Mockito.verify(mockMaster).start();
    Mockito.verify(mockMaster).requestContainers();
    Mockito.verify(mockMaster).stop();
  }

  /**
   * Tests that large container request sizes are handled correctly.
   */
  @Test
  public void bigContainerRequestTest() {
    Configuration conf = new Configuration();
    conf.set(Constants.INTEGRATION_MASTER_RESOURCE_MEM, "128gb");
    conf.set(Constants.INTEGRATION_WORKER_RESOURCE_MEM, "64gb");
    conf.set(Constants.WORKER_MEMORY_SIZE, "256gb");
    ApplicationMaster master = new ApplicationMaster(1, "localhost", "resourcePath", conf);
    Assert.assertEquals(128 * 1024, Whitebox.getInternalState(master, "mMasterMemInMB"));
    Assert.assertEquals(64 * 1024, Whitebox.getInternalState(master, "mWorkerMemInMB"));
    Assert.assertEquals(256 * 1024, Whitebox.getInternalState(master, "mRamdiskMemInMB"));
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
        return ctx.getLocalResources().equals(expectedContext.getLocalResources())
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
