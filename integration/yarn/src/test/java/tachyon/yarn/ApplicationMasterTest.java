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

package tachyon.yarn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Unit tests for {@link ApplicationMaster}.
 */
//TODO(andrew): Add tests for failure cases
@RunWith(PowerMockRunner.class)
@PrepareForTest({NMClient.class, AMRMClientAsync.class, ApplicationMaster.class})
public class ApplicationMasterTest {

  private static final String MASTER_ADDRESS = "localhost";
  private static final String TACHYON_HOME = "/tmp/tachyonhome";
  private static final TachyonConf mConf = new TachyonConf();
  private static final int NUM_WORKERS = 2;
  private static final int MASTER_MEM_MB =
      (int) mConf.getBytes(Constants.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
  private static final int MASTER_CPU = mConf.getInt(Constants.INTEGRATION_MASTER_RESOURCE_CPU);
  private static final int WORKER_MEM_MB =
      (int) mConf.getBytes(Constants.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;
  private static final int RAMDISK_MEM_MB =
      (int) mConf.getBytes(Constants.WORKER_MEMORY_SIZE) / Constants.MB;
  private static final int WORKER_CPU = mConf.getInt(Constants.INTEGRATION_WORKER_RESOURCE_CPU);
  private ApplicationMaster mMaster;
  private ApplicationMasterPrivateAccess mPrivateAccess;
  private NMClient mNMClient;
  private AMRMClientAsync<ContainerRequest> mRMClient;

  @Before
  public void before() throws Exception {
    mMaster = new ApplicationMaster(NUM_WORKERS, TACHYON_HOME, MASTER_ADDRESS);
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

    mMaster.start();
  }

  /**
   * Tests that start() properly registers the application master.
   */
  @Test
  public void testStart() throws Exception {
    String hostname = NetworkAddressUtils.getLocalHostName(new TachyonConf());
    Mockito.verify(mRMClient).registerApplicationMaster(hostname, 0, "");
  }

  /**
   * Tests that the correct type and number of containers are requested.
   */
  @Test
  public void testRequestContainers() throws Exception {
    // Mock the Resource Manager to immediately "allocate" containers after they are requested
    Mockito.doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        if (mPrivateAccess.getMasterAllocated().getCount() == 0) {
          // If the master has already been allocated, this request must be for a worker container
          CountDownLatch workersAllocated = mPrivateAccess.getAllWorkersAllocatedLatch();
          workersAllocated.countDown();
          if (workersAllocated.getCount() == 0) {
            // Once all workers are allocated, we shut down the master so that requestContainers()
            // doesn't run forever
            mMaster.onShutdownRequest();
          }
        } else {
          mPrivateAccess.getMasterAllocated().countDown();
        }
        return null;
      }
    }).when(mRMClient).addContainerRequest(Mockito.any(ContainerRequest.class));

    mMaster.requestContainers();

    // Verify that the right types and numbers of containers were requested
    ArgumentMatcher<ContainerRequest> expectedMasterContainerMatcher =
        new ArgumentMatcher<ContainerRequest>() {
          public boolean matches(Object arg) {
            ContainerRequest expectedMasterContainerRequest =
                new ContainerRequest(Resource.newInstance(MASTER_MEM_MB, MASTER_CPU),
                    new String[] {MASTER_ADDRESS}, null, Priority.newInstance(0), true);
            return EqualsBuilder.reflectionEquals(arg, expectedMasterContainerRequest);
          }
        };
    ArgumentMatcher<ContainerRequest> expectedWorkerContainerMatcher =
        new ArgumentMatcher<ContainerRequest>() {
          public boolean matches(Object arg) {
            ContainerRequest expectedWorkerContainerRequest = new ContainerRequest(
                Resource.newInstance(WORKER_MEM_MB + RAMDISK_MEM_MB, WORKER_CPU), null, null,
                Priority.newInstance(0));
            return EqualsBuilder.reflectionEquals(arg, expectedWorkerContainerRequest);
          }
        };
    Mockito.verify(mRMClient).addContainerRequest(Mockito.argThat(expectedMasterContainerMatcher));
    Mockito.verify(mRMClient, Mockito.times(NUM_WORKERS))
        .addContainerRequest(Mockito.argThat(expectedWorkerContainerMatcher));
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
  }

  /**
   * Tests that the Tachyon master container is launched properly.
   */
  @Test
  public void launchTachyonMasterContainersTest() throws Exception {
    Container mockContainer = Mockito.mock(Container.class);
    Mockito.when(mockContainer.getNodeHttpAddress()).thenReturn("1.2.3.4:8042");

    mMaster.onContainersAllocated(Lists.newArrayList(mockContainer));

    String expectedCommand = new CommandBuilder(
        PathUtils.concatPath(TACHYON_HOME, "integration", "bin", "tachyon-master-yarn.sh"))
            .addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
            .addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr").toString();
    ContainerLaunchContext expectedContext = Records.newRecord(ContainerLaunchContext.class);
    expectedContext.setCommands(Lists.newArrayList(expectedCommand));

    Mockito.verify(mNMClient).startContainer(mockContainer, expectedContext);
    Assert.assertEquals("1.2.3.4", mPrivateAccess.getMasterContainerNetAddress());
    Assert.assertTrue(mPrivateAccess.getMasterAllocated().getCount() == 0);
  }

  /**
   * Tests that the Tachyon worker containers are launched properly.
   */
  @Test
  public void launchTachyonWorkerContainersTest() throws Exception {
    Container mockContainer1 = Mockito.mock(Container.class);
    Container mockContainer2 = Mockito.mock(Container.class);
    // Say that the master is allocated so that container offers are assumed to be worker offers
    mPrivateAccess.getMasterAllocated().countDown();
    mPrivateAccess.setMasterContainerAddress("masterAddress");

    List<Container> containers = Lists.newArrayList(mockContainer1, mockContainer2);

    mMaster.onContainersAllocated(containers);

    String expectedCommand = new CommandBuilder(
        PathUtils.concatPath(TACHYON_HOME, "integration", "bin", "tachyon-worker-yarn.sh"))
            .addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
            .addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr").toString();
    Map<String, String> expectedEnvironment = Maps.newHashMap();
    expectedEnvironment.put("TACHYON_MASTER_ADDRESS", "masterAddress");
    expectedEnvironment.put("TACHYON_WORKER_MEMORY_SIZE", Integer.toString(RAMDISK_MEM_MB) + ".00MB");
    ContainerLaunchContext expectedContext = Records.newRecord(ContainerLaunchContext.class);
    expectedContext.setCommands(Lists.newArrayList(expectedCommand));
    expectedContext.setEnvironment(expectedEnvironment);

    Mockito.verify(mNMClient).startContainer(mockContainer1, expectedContext);
    Mockito.verify(mNMClient).startContainer(mockContainer2, expectedContext);
    Assert.assertEquals(containers.size(),
        NUM_WORKERS - mPrivateAccess.getAllWorkersAllocatedLatch().getCount());
  }

  /**
   * Tests that the main method properly reads args and orchestrates negotiation.
   */
  @Test
  public void mainTest() throws Exception {
    ApplicationMaster mockMaster = Mockito.mock(ApplicationMaster.class);
    PowerMockito.whenNew(ApplicationMaster.class)
        .withArguments(3, "home", "address").thenReturn(mockMaster);

    ApplicationMaster.main(new String[] {"3", "home", "address"});

    Mockito.verify(mockMaster).start();
    Mockito.verify(mockMaster).requestContainers();
    Mockito.verify(mockMaster).stop();
  }

  private static class ApplicationMasterPrivateAccess {
    private final ApplicationMaster mMaster;

    private ApplicationMasterPrivateAccess(ApplicationMaster master) {
      mMaster = master;
    }

    public void setMasterContainerAddress(String address) {
      Whitebox.setInternalState(mMaster, "mMasterContainerNetAddress", address);
    }

    public CountDownLatch getMasterAllocated() {
      return Whitebox.getInternalState(mMaster, "mMasterContainerAllocatedLatch");
    }

    public CountDownLatch getAllWorkersAllocatedLatch() {
      return Whitebox.getInternalState(mMaster, "mAllWorkersAllocatedLatch");
    }

    public String getMasterContainerNetAddress() {
      return Whitebox.getInternalState(mMaster, "mMasterContainerNetAddress");
    }
  }
}
