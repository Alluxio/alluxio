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
 * Entry point for the Alluxio job master program.
 */
@ThreadSafe
public final class AlluxioJobMaster {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobMaster.class);

  /**
   * Starts the Alluxio job master.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioJobMaster.class.getCanonicalName());
      System.exit(-1);
    }

    CommonUtils.PROCESS_TYPE.set(alluxio.util.CommonUtils.ProcessType.JOB_MASTER);
    AlluxioJobMasterProcess process;
    try {
      process = AlluxioJobMasterProcess.Factory.create();
    } catch (Throwable t) {
      LOG.error("Failed to create job master process", t);
      // Exit to stop any non-daemon threads.
      System.exit(-1);
      throw t;
    }

    ProcessUtils.stopProcessOnShutdown(process);
    ProcessUtils.run(process);
  }

  private AlluxioJobMaster() {} // prevent instantiation
}
