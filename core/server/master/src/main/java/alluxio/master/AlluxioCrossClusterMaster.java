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

package alluxio.master;

import alluxio.ProcessUtils;
import alluxio.RuntimeConstants;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio Cross Cluster master program.
 */
@ThreadSafe
public final class AlluxioCrossClusterMaster {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCrossClusterMaster.class);

  /**
   * Starts the Alluxio cross cluster master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioCrossClusterMaster.class.getCanonicalName());
      System.exit(-1);
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.CROSS_CLUSTER_MASTER);
    AlluxioCrossClusterMasterProcess process;
    try {
      process = AlluxioCrossClusterMasterProcess.Factory.create();
    } catch (Throwable t) {
      LOG.error("Failed to create job master process", t);
      // Exit to stop any non-daemon threads.
      System.exit(-1);
      throw t;
    }

    ProcessUtils.stopProcessOnShutdown(process);
    ProcessUtils.run(process);
  }

  private AlluxioCrossClusterMaster() {} // prevent instantiation
}
