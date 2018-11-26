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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.master.MasterInquireClient;
import alluxio.retry.RetryUtils;
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
public final class AlluxioJobWorker {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobWorker.class);

  /**
   * Starts the Alluxio job worker.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioJobWorker.class.getCanonicalName());
      System.exit(-1);
    }

    if (!ConfigurationUtils.masterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio job worker; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Constants.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

    if (!ConfigurationUtils.jobMasterHostConfigured()) {
      System.out.println(String.format(
          "Cannot run alluxio job worker; job master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Constants.SITE_PROPERTIES, PropertyKey.JOB_MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString()));
      System.exit(1);
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.JOB_WORKER);
    MasterInquireClient masterInquireClient = MasterInquireClient.Factory.create();
    try {
      RetryUtils.retry("load cluster default configuration with master", () -> {
        InetSocketAddress masterAddress = masterInquireClient.getPrimaryRpcAddress();
        Configuration.loadClusterDefault(masterAddress);
      }, RetryUtils.defaultWorkerMasterClientRetry());
    } catch (IOException e) {
      ProcessUtils.fatalError(LOG,
          "Failed to load cluster default configuration for job worker: %s", e.getMessage());
    }
    JobWorkerProcess process = JobWorkerProcess.Factory.create();
    ProcessUtils.run(process);
  }

  private AlluxioJobWorker() {} // prevent instantiation
}
