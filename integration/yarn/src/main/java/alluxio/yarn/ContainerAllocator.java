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

import alluxio.exception.ExceptionMessage;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A class for negotiating with YARN to allocate containers.
 *
 * @deprecated since 2.0
 */
@Deprecated
public final class ContainerAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(ContainerAllocator.class);

  private static final int MAX_WORKER_CONTAINER_REQUEST_ATTEMPTS = 20;

  private final String mContainerName;
  private final int mTargetNumContainers;
  private final int mMaxContainersPerHost;
  private final Resource mResource;
  private final String mPreferredHost;
  private final YarnClient mYarnClient;
  private final AMRMClientAsync<ContainerRequest> mRMClient;

  private final ConcurrentHashMultiset<String> mAllocatedContainerHosts;
  private final List<Container> mAllocatedContainers;
  private CountDownLatch mOutstandingContainerRequestsLatch;

  /**
   * @param containerName the name of the type of container being allocated
   * @param targetNumContainers the target number of containers to allocate
   * @param maxContainersPerHost the maximum number of containers to allocate on the same host
   * @param resource the resource to allocate per container
   * @param yarnClient a yarn client for talking to yarn
   * @param rmClient a resource manager client for talking to the yarn resource manager
   */
  public ContainerAllocator(String containerName, int targetNumContainers, int maxContainersPerHost,
      Resource resource, YarnClient yarnClient, AMRMClientAsync<ContainerRequest> rmClient) {
    this(containerName, targetNumContainers, maxContainersPerHost, resource, yarnClient, rmClient,
        null);
  }

  /**
   * @param containerName the name of the type of container being allocated
   * @param targetNumContainers the target number of containers to allocate
   * @param maxContainersPerHost the maximum number of containers to allocate on the same host
   * @param resource the resource to allocate per container
   * @param yarnClient a yarn client for talking to yarn
   * @param rmClient a resource manager client for talking to the yarn resource manager
   * @param preferredHost a specific host to allocate on; if null, any host may be used
   */
  public ContainerAllocator(String containerName, int targetNumContainers, int maxContainersPerHost,
      Resource resource, YarnClient yarnClient,
      AMRMClientAsync<ContainerRequest> rmClient, String preferredHost) {
    mContainerName = containerName;
    mTargetNumContainers = targetNumContainers;
    mMaxContainersPerHost = maxContainersPerHost;
    mResource = resource;
    mPreferredHost = preferredHost;
    mYarnClient = yarnClient;
    mRMClient = rmClient;
    mAllocatedContainerHosts = ConcurrentHashMultiset.create();
    mAllocatedContainers = new ArrayList<>();
  }

  /**
   * @return a list of all hosts in the cluster which haven't yet reached
   *         the containers per host limit
   */
  private String[] getPotentialWorkerHosts() throws YarnException, IOException {
    List<String> unusedHosts = Lists.newArrayList();
    for (String host : YarnUtils.getNodeHosts(mYarnClient)) {
      if (mAllocatedContainerHosts.count(host) < mMaxContainersPerHost) {
        unusedHosts.add(host);
      }
    }
    return unusedHosts.toArray(new String[] {});
  }

  /**
   * Allocates the containers specified by the constructor.
   *
   * @return the allocated containers
   */
  public List<Container> allocateContainers() throws Exception {
    for (int attempt = 0; attempt < MAX_WORKER_CONTAINER_REQUEST_ATTEMPTS; attempt++) {
      LOG.debug("Attempt {} of {} to allocate containers",
          attempt, MAX_WORKER_CONTAINER_REQUEST_ATTEMPTS);
      int numContainersToRequest = mTargetNumContainers - mAllocatedContainerHosts.size();
      LOG.debug("Requesting {} containers", numContainersToRequest);
      mOutstandingContainerRequestsLatch = new CountDownLatch(numContainersToRequest);
      requestContainers();
      // Wait for all outstanding requests to be responded to before beginning the next round.
      mOutstandingContainerRequestsLatch.await();
      if (mAllocatedContainerHosts.size() == mTargetNumContainers) {
        break;
      }
    }
    if (mAllocatedContainers.size() != mTargetNumContainers) {
      throw new RuntimeException(String.format("Failed to allocate %d %s containers",
          mTargetNumContainers, mContainerName));
    }
    return mAllocatedContainers;
  }

  /**
   * @param container the container which has been allocated by YARN
   */
  public synchronized void allocateContainer(Container container) {
    String containerHost = container.getNodeId().getHost();
    if (mAllocatedContainerHosts.count(containerHost) < mMaxContainersPerHost
        && mAllocatedContainerHosts.size() < mTargetNumContainers) {
      mAllocatedContainerHosts.add(containerHost);
      mAllocatedContainers.add(container);
    } else {
      LOG.info("Releasing assigned container on host {}", containerHost);
      mRMClient.releaseAssignedContainer(container.getId());
    }
    mOutstandingContainerRequestsLatch.countDown();
  }

  private void requestContainers() throws Exception {
    String[] hosts;
    boolean relaxLocality;
    // YARN requires that priority for relaxed-locality requests is different from strict-locality.
    Priority priority;
    if (mPreferredHost != null) {
      hosts = new String[]{mPreferredHost};
      relaxLocality = mPreferredHost.equals("any");
      priority = Priority.newInstance(100);
    } else {
      hosts = getPotentialWorkerHosts();
      relaxLocality = true;
      priority = Priority.newInstance(101);
    }

    int numContainersToRequest = mTargetNumContainers - mAllocatedContainers.size();
    LOG.info("Requesting {} {} containers", numContainersToRequest, mContainerName);

    if (hosts.length * mMaxContainersPerHost < numContainersToRequest) {
      throw new RuntimeException(ExceptionMessage.YARN_NOT_ENOUGH_HOSTS
          .getMessage(numContainersToRequest, mContainerName, hosts.length));
    }

    ContainerRequest containerRequest = new ContainerRequest(mResource, hosts,
        null /* any racks */, priority, relaxLocality);
    LOG.info(
        "Making {} resource request(s) for Alluxio {}s with cpu {} memory {}MB on hosts {}",
        numContainersToRequest, mContainerName,
        mResource.getVirtualCores(), mResource.getMemory(), hosts);
    for (int i = 0; i < numContainersToRequest; i++) {
      mRMClient.addContainerRequest(containerRequest);
    }
  }
}
