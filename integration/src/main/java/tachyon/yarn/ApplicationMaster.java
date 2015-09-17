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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

/**
 * Actual owner of Tachyon running on Yarn. The YARN ResourceManager will launch this
 * ApplicationMaster on an allocated container. The ApplicationMaster communicates with YARN
 * cluster, and handles application execution. It performs operations in an asynchronous fashion.
 */
public final class ApplicationMaster implements AMRMClientAsync.CallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final int mMasterCpu;
  private final int mWorkerCpu;
  private final int mMasterMem;
  private final int mWorkerMem;
  private final int mNumWorkers;

  private final String mTachyonHome;
  private final YarnConfiguration mYarnConf = new YarnConfiguration();
  private final TachyonConf mTachyonConf = new TachyonConf();
  /** Client to talk to Resource Manager */
  private AMRMClientAsync<ContainerRequest> mRMClient;
  /** Client to talk to Node Manager */
  private NMClient mNMClient;
  /** Whether a container for Tachyon master is allocated */
  private volatile boolean mMasterContainerAllocated;
  /** Num of allocated worker containers */
  private volatile int mNumAllocatedWorkerContainers;
  /** Network address of the container allocated for Tachyon master */
  private String mMasterContainerNetAddress;

  public ApplicationMaster(int numWorkers, String tachyonHome) {
    mMasterCpu = mTachyonConf.getInt(Constants.MASTER_RESOURCE_CPU);
    mMasterMem = (int) mTachyonConf.getBytes(Constants.MASTER_RESOURCE_MEM) / Constants.MB;
    mWorkerCpu = mTachyonConf.getInt(Constants.WORKER_RESOURCE_CPU);
    mWorkerMem = (int) mTachyonConf.getBytes(Constants.WORKER_RESOURCE_MEM) / Constants.MB;
    mNumWorkers = numWorkers;
    mTachyonHome = tachyonHome;
    mMasterContainerAllocated = false;
    mNumAllocatedWorkerContainers = 0;
  }

  /**
   * @param args Command line arguments to launch application master
   */
  public static void main(String[] args) {
    try {
      LOG.info("Starting Application Master with args " + Arrays.toString(args));
      final int numWorkers = Integer.valueOf(args[0]);
      final String tachyonHome = args[1];
      ApplicationMaster applicationMaster = new ApplicationMaster(numWorkers, tachyonHome);
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
    if (!mMasterContainerAllocated) {
      launchTachyonMasterContainers(containers);
    } else {
      launchTachyonWorkerContainers(containers);
    }
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> statuses) {
    for (ContainerStatus status : statuses) {
      LOG.error("Completed container " + status.getContainerId());
    }
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updated) {}

  @Override
  public void onShutdownRequest() {}

  @Override
  public void onError(Throwable t) {}

  @Override
  public float getProgress() {
    return 0;
  }

  public void start() throws IOException, YarnException {
    // create a client to talk to NM
    mNMClient = NMClient.createNMClient();
    mNMClient.init(mYarnConf);
    mNMClient.start();

    // Create a client to talk to the RM
    mRMClient = AMRMClientAsync.createAMRMClientAsync(100, this);
    mRMClient.init(mYarnConf);
    mRMClient.start();

    // Register with ResourceManager
    mRMClient.registerApplicationMaster("" /* hostname */, 0 /* port */, "" /* tracking url */);
    LOG.info("ApplicationMaster registered");
  }

  public void requestContainers() throws Exception {
    // Priority for Tachyon master and worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for master containers
    Resource masterResource = Records.newRecord(Resource.class);
    masterResource.setMemory(mMasterMem);
    masterResource.setVirtualCores(mMasterCpu);

    // Make container request for Tachyon master to ResourceManager
    ContainerRequest masterContainerAsk =
        new ContainerRequest(masterResource, null /* any hosts */, null /* any racks */, priority);
    LOG.info("Making resource request for Tachyon master");
    mRMClient.addContainerRequest(masterContainerAsk);

    // Wait until Tachyon master container has been allocated
    while (!mMasterContainerAllocated) {
      Thread.sleep(1000);
    }

    // Resource requirements for master containers
    Resource workerResource = Records.newRecord(Resource.class);
    workerResource.setMemory(mWorkerMem);
    workerResource.setVirtualCores(mWorkerCpu);

    // Make container requests for workers to ResourceManager
    for (int i = 0; i < mNumWorkers; i ++) {
      ContainerRequest containerAsk =
          new ContainerRequest(workerResource, null /* any hosts */, null /* any racks */,
              priority);
      LOG.info("Making resource request for Tachyon worker " + i);
      mRMClient.addContainerRequest(containerAsk);
    }

    // Wait until all Tachyon worker containers have been allocated
    while (mNumAllocatedWorkerContainers < mNumWorkers) {
      Thread.sleep(1000);
    }

    // Wait for 5 more seconds to avoid application unregistered before some container fully
    // launched.
    Thread.sleep(5000);
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
  }

  private void launchTachyonMasterContainers(List<Container> containers) {
    String startScript = PathUtils.concatPath(mTachyonHome, "bin", "tachyon-start.sh");
    String command =
        startScript + " master" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
            + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

    for (Container container : containers) {
      try {
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(command));
        LOG.info("Launching container " + container.getId() + " for Tachyon master");
        LOG.info("Master command: " + command);
        mNMClient.startContainer(container, ctx);
        String containerUri = container.getNodeHttpAddress(); // in the form of 1.2.3.4:8042
        mMasterContainerNetAddress = containerUri.split(":")[0];
        LOG.info("Master address: " + mMasterContainerNetAddress);
        mMasterContainerAllocated = true;
        return;
      } catch (Exception ex) {
        LOG.error("Error launching container " + container.getId() + " " + ex);
      }
    }
  }

  private void launchTachyonWorkerContainers(List<Container> containers) {
    String startScript = PathUtils.concatPath(mTachyonHome, "bin", "tachyon-start.sh");
    String command =
        startScript + " worker SudoMount" + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
            + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";

    Map<String, String> environmentMap = new HashMap<String, String>();
    environmentMap.put("TACHYON_MASTER_ADDRESS", mMasterContainerNetAddress);
    environmentMap.put("TACHYON_WORKER_MEMORY_SIZE",
        FormatUtils.getSizeFromBytes((long) mWorkerMem * Constants.MB));

    for (Container container : containers) {
      try {
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(command));
        ctx.setEnvironment(environmentMap);
        LOG.debug("environment " + environmentMap.toString());
        LOG.info("Launching container " + container.getId() + " for Tachyon worker");
        LOG.info("Worker command: " + command);
        mNMClient.startContainer(container, ctx);
        mNumAllocatedWorkerContainers ++;
      } catch (Exception ex) {
        LOG.error("Error launching container " + container.getId() + " " + ex);
      }
    }
  }
}
