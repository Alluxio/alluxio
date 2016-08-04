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

import alluxio.Constants;
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
 *
 */
public final class ContainerAllocator {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int MAX_WORKER_CONTAINER_REQUEST_ATTEMPTS = 20;

  private static final Priority PRIORITY = Priority.newInstance(0);

  private final String mContainerName;
  private final int mTargetNumContainers;
  private final int mMaxContainersPerHost;
  private final Resource mResource;
  private final String mPreferredHost;
  private final YarnClient mYarnClient;
  private final AMRMClientAsync<ContainerRequest> mRMClient;

  private final ConcurrentHashMultiset<String> mAllocatedContainerHosts;
  private final List<Container> mAllocatedContainers;
  private int mNumRunningContainers;
  private CountDownLatch mOutstandingContainerRequestsLatch;

  public ContainerAllocator(String containerName, int targetNumContainers, int maxContainersPerHost,
      Resource resource, YarnClient yarnClient, AMRMClientAsync<ContainerRequest> rmClient) {
    this(containerName, targetNumContainers, maxContainersPerHost, resource, yarnClient, rmClient,
        null);
  }

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
    mNumRunningContainers = 0;
  }

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
   * @throws Exception if an error occurs
   */
  public List<Container> allocateContainers() throws Exception {
    for (int attempt = 0; attempt < MAX_WORKER_CONTAINER_REQUEST_ATTEMPTS; attempt++) {
      int numContainersToRequest = mTargetNumContainers - mNumRunningContainers;
      mOutstandingContainerRequestsLatch = new CountDownLatch(numContainersToRequest);
      requestContainers(numContainersToRequest);
      // Wait for all outstanding requests to be responded to before beginning the next round.
      mOutstandingContainerRequestsLatch.await();
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
    mOutstandingContainerRequestsLatch.countDown();
    String containerHost = container.getNodeId().getHost();
    if (mAllocatedContainerHosts.count(containerHost) < mMaxContainersPerHost
        && mAllocatedContainerHosts.size() < mTargetNumContainers) {
      mAllocatedContainerHosts.add(containerHost);
      mAllocatedContainers.add(container);
    } else {
      LOG.info("Releasing assigned container on host {}", containerHost);
      mRMClient.releaseAssignedContainer(container.getId());
    }
  }

  private void requestContainers(int numContainersToRequest) throws Exception {
    LOG.info("Requesting {} {} containers", numContainersToRequest, mContainerName);
    boolean relaxLocality;
    String[] hosts;
    if (mPreferredHost != null) {
      relaxLocality = false;
      hosts = new String[]{mPreferredHost};
    } else {
      relaxLocality = true;
      hosts = getPotentialWorkerHosts();
    }

    if (hosts.length * mMaxContainersPerHost < numContainersToRequest) {
      throw new RuntimeException(
          ExceptionMessage.YARN_NOT_ENOUGH_HOSTS.getMessage(numContainersToRequest, hosts.length));
    }

    ContainerRequest containerRequest = new ContainerRequest(mResource, hosts,
        null /* any racks */, PRIORITY, relaxLocality);
    LOG.info(
        "Making {} resource request(s) for Alluxio workers with cpu {} memory {}MB on hosts {}",
        numContainersToRequest, mResource.getVirtualCores(), mResource.getMemory(), hosts);
    for (int i = mNumRunningContainers; i < mTargetNumContainers; i++) {
      mRMClient.addContainerRequest(containerRequest);
    }
  }
}
