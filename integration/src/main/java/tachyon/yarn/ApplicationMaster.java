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
import java.util.Collections;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Actual owner of Tachyon running on Yarn. The YARN ResourceManager will launch this
 * ApplicationMaster on an allocated container. The ApplicationMaster communicates with YARN
 * cluster, and handles application execution. It performs operations in a synchronous fashion.
 */
public final class ApplicationMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private YarnConfiguration mYarnConf = new YarnConfiguration();
  private TachyonConf mTachyonConf = new TachyonConf();

  /** Client to talk to Resource Manager */
  private AMRMClient<ContainerRequest> mRMClient;
  /** Client to talk to Node Manager */
  private NMClient mNMClient;


  public ApplicationMaster() {}

  /**
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    try {
      LOG.info("Starting AM with args " + args);
      final int numWorkers = Integer.valueOf(args[0]);
      ApplicationMaster applicationMaster = new ApplicationMaster();
      applicationMaster.start();
      applicationMaster.startContainers(numWorkers);
      applicationMaster.stop();
    } catch (Exception e) {
      LOG.error("Error running Application Master " + e);
      System.exit(1);
    }
  }

  public void start() throws IOException, YarnException {
    // Create a client to talk to the RM
    mRMClient = AMRMClient.createAMRMClient();
    mRMClient.init(mYarnConf);
    mRMClient.start();
    mRMClient.registerApplicationMaster("" /* hostname */, 0 /* port */, "" /* tracking url */);

    // create a client to talk to NM
    mNMClient = NMClient.createNMClient();
    mNMClient.init(mYarnConf);
    mNMClient.start();
  }

  public void startContainers(int numWorkers) throws Exception {
    int masterCpu = mTachyonConf.getInt(Constants.MASTER_RESOURCE_CPU);
    int masterMem = (int) mTachyonConf.getBytes(Constants.MASTER_RESOURCE_MEM) / Constants.MB;
    int workerCpu = mTachyonConf.getInt(Constants.WORKER_RESOURCE_CPU);
    int workerMem = (int) mTachyonConf.getBytes(Constants.WORKER_RESOURCE_MEM) / Constants.MB;

    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);

    // Resource requirements for master containers
    Resource masterCapability = Records.newRecord(Resource.class);
    masterCapability.setMemory(masterMem);
    masterCapability.setVirtualCores(masterCpu);
    // Resource requirements for master containers
    Resource workerCapability = Records.newRecord(Resource.class);
    workerCapability.setMemory(workerMem);
    workerCapability.setVirtualCores(workerCpu);

    ContainerRequest masterContainerAsk = new ContainerRequest(masterCapability,
        null /* any hosts */, null /* any racks */, priority);
    LOG.info("Making resource request for tachyon master");
    mRMClient.addContainerRequest(masterContainerAsk);

    // Make container requests to ResourceManager
    for (int i = 0; i < numWorkers; i ++) {
      ContainerRequest containerAsk = new ContainerRequest(workerCapability, null, null, priority);
      LOG.info("Making resource request for tachyon worker " + i);
      mRMClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers, launch and check for responses
    int responseId = 0;
    int completedContainers = 0;
    while (completedContainers < numWorkers) {
      AllocateResponse response = mRMClient.allocate(responseId ++);
      for (Container container : response.getAllocatedContainers()) {
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        String command = "/bin/sh -c echo \" hello \"";
        ctx.setCommands(
            Collections.singletonList(command + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
                + "/stdout" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        LOG.info("Launching container " + container.getId());
        mNMClient.startContainer(container, ctx);
      }
      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        completedContainers ++;
        LOG.info("Completed container " + status.getContainerId());
      }
      Thread.sleep(100);
    }
  }

  public void stop() {
    try {
      mRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    } catch (YarnException ye) {
      LOG.error("Failed to unregister application " + ye);
    } catch (IOException ioe) {
      LOG.error("Failed to unregister application " + ioe);
    }
    mRMClient.stop();
  }

}
