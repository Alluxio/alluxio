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

package alluxio.worker;

import alluxio.conf.ServerConfiguration;
import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.MasterInquireClient;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio worker.
 */
@ThreadSafe
public final class AlluxioWorker {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorker.class);

  /**
   * Starts the Alluxio worker.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioWorker.class.getCanonicalName());
      System.exit(-1);
    }

    if (!ConfigurationUtils.masterHostConfigured(ServerConfiguration.global())) {
      ProcessUtils.fatalError(LOG,
          ConfigurationUtils.getMasterHostNotConfiguredMessage("Alluxio worker"));
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.WORKER);
    MasterInquireClient masterInquireClient =
        MasterInquireClient.Factory.create(ServerConfiguration.global(), ServerUserState.global());
    try {
      RetryUtils.retry("load cluster default configuration with master", () -> {
        InetSocketAddress masterAddress = masterInquireClient.getPrimaryRpcAddress();
        ServerConfiguration.loadWorkerClusterDefaults(masterAddress);
      }, RetryUtils.defaultWorkerMasterClientRetry(
          ServerConfiguration.getDuration(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)));
    } catch (IOException e) {
      ProcessUtils.fatalError(LOG,
          "Failed to load cluster default configuration for worker. Please make sure that Alluxio "
              + "master is running: %s", e.toString());
    }
    WorkerProcess process;
    try {
      process = WorkerProcess.Factory.create();
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to create worker process");
      // fatalError will exit, so we shouldn't reach here.
      throw t;
    }

    ProcessUtils.stopProcessOnShutdown(process);
    ProcessUtils.run(process);
  }

  private AlluxioWorker() {} // prevent instantiation
}
