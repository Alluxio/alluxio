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
 * Entry point for the Alluxio master.
 */
@ThreadSafe
public final class AlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMaster.class);

  /**
   * Starts the Alluxio master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioMaster.class.getCanonicalName());
      System.exit(-1);
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.MASTER);
    MasterProcess process;
    try {
      process = AlluxioMasterProcess.Factory.create();
    } catch (Throwable t) {
      ProcessUtils.fatalError(LOG, t, "Failed to create master process");
      // fatalError will exit, so we shouldn't reach here.
      throw t;
    }

    // Register a shutdown hook for master, so that master closes the journal files when it
    // receives SIGTERM.
    ProcessUtils.stopProcessOnShutdown(process);
    ProcessUtils.run(process);
  }

  private AlluxioMaster() {} // prevent instantiation
}
