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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Actual owner of Tachyon running on Yarn. The YARN ResourceManager will launch this
 * ApplicationMaster on an allocated container. The ApplicationMaster communicates with the YARN
 * cluster, and handles application execution. It performs operations asynchronously.
 */
public final class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Maximum number of rounds of requesting and re-requesting worker containers */
  // TODO(andrew): make this configurable
  private static final int MAX_WORKER_CONTAINER_REQUEST_ROUNDS = 20;

  // Container request priorities are intra-application
  private static final Priority MASTER_PRIORITY = Priority.newInstance(0);
  // We set master and worker container request priorities to different values because
  // Yarn doesn't allow both relaxed locality and non-relaxed locality requests to be made
  // at the same priority level
  private static final Priority WORKER_PRIORITY = Priority.newInstance(1);

  // Parameters sent from Client
  private final int mMasterCpu;
  private final int mWorkerCpu;
  private final int mMasterMemInMB;
  private final int mWorkerMemInMB;
  private final int mRamdiskMemInMB;
  private final int mNumWorkers;
  private final String mTachyonHome;
  private final String mMasterAddress;

  /** Set of hostnames for launched workers */
  private final Set<String> mWorkerHosts;
  private final YarnConfiguration mYarnConf = new YarnConfiguration();
  private final TachyonConf mTachyonConf = new TachyonConf();
  /** The count starts at 1, then becomes 0 when we allocate a container for the Tachyon master */
  private final CountDownLatch mMasterContainerAllocatedLatch;
  /** The count starts at 1, then becomes 0 when the application is done */
  private final CountDownLatch mApplicationDoneLatch;

  /** Client to talk to Resource Manager */
  private AMRMClientAsync<ContainerRequest> mRMClient;
  /** Client to talk to Node Manager */
  private NMClient mNMClient;
  /** Client Resource Manager Service */
  private YarnClient mYarnClient;
  /** Network address of the container allocated for Tachyon master */
  private String mMasterContainerNetAddress;
  /** The number of worker container requests we are waiting to hear back from */
  private AtomicInteger mOutstandingWorkerContainerRequests;

  public ApplicationMaster(int numWorkers, String tachyonHome, String masterAddress) {
    mMasterCpu = mTachyonConf.getInt(Constants.INTEGRATION_MASTER_RESOURCE_CPU);
    mMasterMemInMB =
        (int) mTachyonConf.getBytes(Constants.INTEGRATION_MASTER_RESOURCE_MEM) / Constants.MB;
    mWorkerCpu = mTachyonConf.getInt(Constants.INTEGRATION_WORKER_RESOURCE_CPU);
    // TODO(binfan): request worker container and ramdisk container separately
    // memory for running worker
    mWorkerMemInMB =
        (int) mTachyonConf.getBytes(Constants.INTEGRATION_WORKER_RESOURCE_MEM) / Constants.MB;
    // memory for running ramdisk
    mRamdiskMemInMB = (int) mTachyonConf.getBytes(Constants.WORKER_MEMORY_SIZE) / Constants.MB;
    mNumWorkers = numWorkers;
    mTachyonHome = tachyonHome;
    mMasterAddress = masterAddress;
    mWorkerHosts = Sets.newHashSet();
    mMasterContainerAllocatedLatch = new CountDownLatch(1);
    mApplicationDoneLatch = new CountDownLatch(1);
    mOutstandingWorkerContainerRequests = new AtomicInteger(0);
  }

  /**
   * @param args Command line arguments to launch application master
   */
  public static void main(String[] args) {
    Preconditions.checkArgument(args[1] != null, "Tachyon home cannot be null");
    Preconditions.checkArgument(args[2] != null, "Address of Tachyon master cannot be null");
    try {
      LOG.info("Starting Application Master with args " + Arrays.toString(args));
      final int numWorkers = Integer.parseInt(args[0]);
      final String tachyonHome = args[1];
      final String masterAddress = args[2];
      ApplicationMaster applicationMaster =
          new ApplicationMaster(numWorkers, tachyonHome, masterAddress);
      applicationMaster.start();
      applicationMaster.requestContainers();
      applicationMaster.stop();
    } catch (Exception ex) {
      LOG.error("Error running Application Master " + ex);
      System.exit(1);
    }
  }

  @Override
  public void onContainersAllocated(List<Container> containers) {
    if (mMasterContainerAllocatedLatch.getCount() != 0) {
      launchTachyonMasterContainers(containers);
    } else {
      launchTachyonWorkerContainers(containers);
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus status : statuses) {
      // Releasing worker containers because we already have workers on their host will generate a
      // callback to this method, so we use warn instead of error.
      LOG.info("Completed container " + status.getContainerId() + " state: " + status.getState());
    }
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updated) {}

  @Override
  public void onShutdownRequest() {
    mApplicationDoneLatch.countDown();
  }

  @Override
  public void onError(Throwable t) {}

  @Override
  public float getProgress() {
    return 0;
  }

  public void start() throws IOException, YarnException {
    // create a client to talk to NodeManager
    mNMClient = NMClient.createNMClient();
    mNMClient.init(mYarnConf);
    mNMClient.start();

    // Create a client to talk to the ResourceManager
    mRMClient = AMRMClientAsync.createAMRMClientAsync(100, this);
    mRMClient.init(mYarnConf);
    mRMClient.start();

    // Create a client to talk to Yarn e.g. to find out what nodes exist in the cluster
    mYarnClient = YarnClient.createYarnClient();
    mYarnClient.init(mYarnConf);
    mYarnClient.start();

    // Register with ResourceManager
    String hostname = NetworkAddressUtils.getLocalHostName(new TachyonConf());
    mRMClient.registerApplicationMaster(hostname, 0 /* port */, "" /* tracking url */);
    LOG.info("ApplicationMaster registered");
  }

  public void requestContainers() throws Exception {
    requestMasterContainer();

    // Wait until all Tachyon worker containers have been allocated
    int round = 0;
    synchronized (mWorkerHosts) {
      while (mWorkerHosts.size() < mNumWorkers && round < MAX_WORKER_CONTAINER_REQUEST_ROUNDS) {
        requestWorkerContainers();
        round ++;
        while (mOutstandingWorkerContainerRequests.get() > 0) {
          LOG.info("Waiting for worker containers to be allocated");
          mWorkerHosts.wait();
        }
      }
      if (mWorkerHosts.size() < mNumWorkers) {
        LOG.error(
            "Could not request {} workers from yarn resource manager after {} tries. "
                + "Proceeding with {} workers",
            mNumWorkers, MAX_WORKER_CONTAINER_REQUEST_ROUNDS, mWorkerHosts.size());
      }
    }

    LOG.info("Master and workers are launched");
    mApplicationDoneLatch.await();
  }

  /**
   * Requests a container for the master and waits for it to be allocated.
   */
  private void requestMasterContainer() throws Exception {
    LOG.info("Requesting master container");
    // Resource requirements for master containers
    Resource masterResource = Records.newRecord(Resource.class);
    masterResource.setMemory(mMasterMemInMB);
    masterResource.setVirtualCores(mMasterCpu);

    String[] nodes = {mMasterAddress};

    // Make container request for Tachyon master to ResourceManager
    boolean relaxLocality = true;
    if (!mMasterAddress.equals("localhost")) {
      relaxLocality = false;
    }
    ContainerRequest masterContainerAsk = new ContainerRequest(masterResource, nodes,
        null /* any racks */, MASTER_PRIORITY, relaxLocality);
    LOG.info("Making resource request for Tachyon master: cpu {} memory {} MB on node {}",
        masterResource.getVirtualCores(), masterResource.getMemory(), mMasterAddress);
    mRMClient.addContainerRequest(masterContainerAsk);

    LOG.info("Waiting for master container to be allocated");
    mMasterContainerAllocatedLatch.await();
  }

  /**
   * Requests containers for the workers, attempting to get containers on separate nodes.
   */
  private void requestWorkerContainers() throws Exception {
    LOG.info("Requesting worker containers");
    // Resource requirements for worker containers
    Resource workerResource = Records.newRecord(Resource.class);
    workerResource.setMemory(mWorkerMemInMB + mRamdiskMemInMB);
    workerResource.setVirtualCores(mWorkerCpu);
    int currentNumWorkers;
    synchronized (mWorkerHosts) {
      currentNumWorkers = mWorkerHosts.size();
    }

    // Make container requests for workers to ResourceManager
    for (int i = currentNumWorkers; i < mNumWorkers; i ++) {
      ContainerRequest containerAsk = new ContainerRequest(workerResource, getUnusedWorkerHosts(),
          null /* any racks */, WORKER_PRIORITY, false /* demand only unused workers */);
      LOG.info("Making resource request for Tachyon worker {}: cpu {} memory {} MB on any nodes", i,
          workerResource.getVirtualCores(), workerResource.getMemory());
      mOutstandingWorkerContainerRequests.incrementAndGet();
      mRMClient.addContainerRequest(containerAsk);
    }
  }

  /**
   * @return the hostnames in the cluster which are not being used by a Tachyon worker
   */
  private String[] getUnusedWorkerHosts() throws Exception {
    List<String> unusedHosts = Lists.newArrayList();
    synchronized (mWorkerHosts) {
      for (String host : YarnUtils.getNodeHosts(mYarnClient)) {
        if (!mWorkerHosts.contains(host)) {
          unusedHosts.add(host);
        }
      }
    }
    return unusedHosts.toArray(new String[] {});
  }

  public void stop() {
    try {
      mRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    } catch (YarnException yex) {
      LOG.error("Failed to unregister application " + yex);
    } catch (IOException ioe) {
      LOG.error("Failed to unregister application " + ioe);
    }
    mRMClient.stop();
    // TODO(andrew): Think about whether we should stop mNMClient here
    mYarnClient.stop();
  }

  private void launchTachyonMasterContainers(List<Container> containers) {
    if (containers.size() == 0) {
      LOG.warn("launchTachyonMasterContainers was called with no containers");
      return;
    } else if (containers.size() >= 2) {
      // NOTE: We can remove this check if we decide to support YARN multi-master in the future
      LOG.warn("{} containers were allocated for the Tachyon Master. Ignoring all but one.",
          containers.size());
    }

    Container container = containers.get(0);

    final String command = new CommandBuilder(
        PathUtils.concatPath(mTachyonHome, "integration", "bin", "tachyon-master-yarn.sh"))
            .addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
            .addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr").toString();

    List<String> commands = Lists.newArrayList(command);

    try {
      ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
      ctx.setCommands(commands);
      LOG.info("Launching container {} for Tachyon master on {} with master command: {}",
          container.getId(), container.getNodeHttpAddress(), commands);
      mNMClient.startContainer(container, ctx);
      String containerUri = container.getNodeHttpAddress(); // in the form of 1.2.3.4:8042
      mMasterContainerNetAddress = containerUri.split(":")[0];
      LOG.info("Master address: " + mMasterContainerNetAddress);
      mMasterContainerAllocatedLatch.countDown();
      return;
    } catch (Exception ex) {
      LOG.error("Error launching container " + container.getId() + " " + ex);
    }
  }

  private void launchTachyonWorkerContainers(List<Container> containers) {
    final String command = new CommandBuilder(
        PathUtils.concatPath(mTachyonHome, "integration", "bin", "tachyon-worker-yarn.sh"))
            .addArg("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout")
            .addArg("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr").toString();

    List<String> commands = Lists.newArrayList(command);
    Map<String, String> environmentMap = new HashMap<String, String>();
    environmentMap.put("TACHYON_MASTER_ADDRESS", mMasterContainerNetAddress);
    environmentMap.put("TACHYON_WORKER_MEMORY_SIZE",
        FormatUtils.getSizeFromBytes((long) mRamdiskMemInMB * Constants.MB));

    for (Container container : containers) {
      synchronized (mWorkerHosts) {
        if (mWorkerHosts.size() >= mNumWorkers
            || mWorkerHosts.contains(container.getNodeId().getHost())) {
          LOG.info("Releasing assigned container on {}", container.getNodeId().getHost());
          // Avoid re-using nodes - we don't support multiple workers on the same node
          mRMClient.releaseAssignedContainer(container.getId());
        } else {
          try {
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
            ctx.setCommands(commands);
            ctx.setEnvironment(environmentMap);
            LOG.info("Launching container {} for Tachyon worker {} on {} with worker command: {}",
                container.getId(), mWorkerHosts.size(), container.getNodeHttpAddress(), command);
            mNMClient.startContainer(container, ctx);
            mWorkerHosts.add(container.getNodeId().getHost());
          } catch (Exception ex) {
            LOG.error("Error launching container " + container.getId() + " " + ex);
          }
        }
        mOutstandingWorkerContainerRequests.decrementAndGet();
        mWorkerHosts.notify();
      }
    }
  }
}
