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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.exception.ExceptionMessage;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Unit tests for {@link ContainerAllocator}.
 */
public final class ContainerAllocatorTest {
  private static final String CONTAINER_NAME = "test";

  private Resource mResource;
  private YarnClient mYarnClient;
  private AMRMClientAsync<ContainerRequest> mRMClient;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    mResource = mock(Resource.class);
    mYarnClient = mock(YarnClient.class);
    mRMClient = (AMRMClientAsync<ContainerRequest>) mock(AMRMClientAsync.class);
  }

  @Test(timeout = 10000)
  public void oneContainerPerHostFullAllocation() throws Exception {
    int numHosts = 10;
    int maxContainersPerHost = 1;
    testFullAllocation(numHosts, maxContainersPerHost);
  }

  @Test(timeout = 10000)
  public void fiveContainersPerHostFullAllocation() throws Exception {
    int numHosts = 10;
    int maxContainersPerHost = 5;
    testFullAllocation(numHosts, maxContainersPerHost);
  }

  @Test(timeout = 10000)
  public void fiveContainersPerHostHalfAllocation() throws Exception {
    int numHosts = 10;
    int maxContainersPerHost = 5;
    int numContainers = numHosts * maxContainersPerHost / 2;
    ContainerAllocator containerAllocator =
        setup(numHosts, maxContainersPerHost, numContainers);
    List<Container> containers = containerAllocator.allocateContainers();

    assertEquals(numContainers, containers.size());
    checkMaxHostsLimitNotExceeded(containers, maxContainersPerHost);
  }

  @Test(timeout = 10000)
  public void notEnoughHosts() throws Exception {
    int numHosts = 10;
    int maxContainersPerHost = 5;
    int numContainers = numHosts * maxContainersPerHost + 1; // one container too many
    ContainerAllocator containerAllocator =
        setup(numHosts, maxContainersPerHost, numContainers);
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage(
        ExceptionMessage.YARN_NOT_ENOUGH_HOSTS.getMessage(numContainers, CONTAINER_NAME, numHosts));
    containerAllocator.allocateContainers();
  }

  @Test(timeout = 1000)
  public void allocateMasterInAnyHost() throws Exception {
    ContainerAllocator containerAllocator = new ContainerAllocator(CONTAINER_NAME, 1,
        1, mResource, mYarnClient, mRMClient, "any");
    doAnswer(allocateFirstHostAnswer(containerAllocator))
        .when(mRMClient).addContainerRequest(Matchers.argThat(request -> {
          if (request.getRelaxLocality() == true
              && request.getNodes().size() == 1
              && request.getNodes().get(0).equals("any")) {
            return true;
          }
          return false;
        }));
    containerAllocator.allocateContainers();
  }

  /*
   * Creates a container allocator for allocating the specified numContainers with the specified
   * maxContainersPerHost.
   *
   * The yarn client is mocked to make it look like there are numHosts different hosts in the
   * system, and the resource manager client is mocked to allocate containers when they are
   * requested.
   */
  private ContainerAllocator setup(int numHosts, int maxContainersPerHost, int numContainers)
      throws Exception {
    ContainerAllocator containerAllocator = new ContainerAllocator(CONTAINER_NAME, numContainers,
        maxContainersPerHost, mResource, mYarnClient, mRMClient);
    List<NodeReport> nodeReports = new ArrayList<>();
    for (int i = 0; i < numHosts; i++) {
      NodeReport nodeReport = Records.newRecord(NodeReport.class);
      nodeReport.setNodeId(NodeId.newInstance("host" + i, 0));
      nodeReports.add(nodeReport);
    }
    when(mYarnClient.getNodeReports(Matchers.<NodeState[]>anyVararg())).thenReturn(nodeReports);
    doAnswer(allocateFirstHostAnswer(containerAllocator)).when(mRMClient)
        .addContainerRequest(any(ContainerRequest.class));
    return containerAllocator;
  }

  /*
   * Tests that when there are the specified number of hosts and the specified max containers per
   * host, a ContainerAllocator can fully allocate all hosts so that every host has
   * maxContainersPerHost containers.
   */
  private void testFullAllocation(int numHosts, int maxContainersPerHost) throws Exception {
    int numContainers = numHosts * maxContainersPerHost;
    ContainerAllocator containerAllocator = setup(numHosts, maxContainersPerHost, numContainers);
    List<Container> containers = containerAllocator.allocateContainers();

    Set<String> containerHosts = new HashSet<>();
    for (Container container : containers) {
      containerHosts.add(container.getNodeId().getHost());
    }
    assertEquals("All hosts are allocated", numHosts, containerHosts.size());
    assertEquals("All containers are allocated", numContainers, containers.size());
    checkMaxHostsLimitNotExceeded(containers, maxContainersPerHost);
  }

  /*
   * Creates an Answer to an addContainerRequest method. The Answer picks the first node requested
   * and allocates a container on it, sending a callback to the specified container allocator
   */
  private Answer<Void> allocateFirstHostAnswer(final ContainerAllocator containerAllocator) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ContainerRequest containerRequest = invocation.getArgument(0, ContainerRequest.class);
        Container container = Records.newRecord(Container.class);
        container.setNodeId(NodeId.newInstance(containerRequest.getNodes().get(0), 0));
        containerAllocator.allocateContainer(container);
        return null;
      }
    };
  }

  /*
   * Verifies that no more than maxContainersPerHost of the containers are located on the same host.
   */
  private void checkMaxHostsLimitNotExceeded(List<Container> containers, int maxContainersPerHost) {
    ConcurrentHashMap<String, Integer> counts = new ConcurrentHashMap<>();
    for (Container container : containers) {
      String host = container.getNodeId().getHost();
      counts.putIfAbsent(host, 0);
      int newCount = counts.get(host) + 1;
      assertTrue(newCount <= maxContainersPerHost);
      counts.put(host, newCount);
    }
  }
}
