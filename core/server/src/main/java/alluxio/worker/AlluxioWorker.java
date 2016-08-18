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

import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for running an Alluxio worker.
 */
public final class AlluxioWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Starts the Alluxio worker.
   *
   * A block worker will be started and the Alluxio worker will continue to run until the block
   * worker thread exits.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioWorker.class.getCanonicalName());
      System.exit(-1);
    }

    // validate the configuration
    if (!ConfigurationUtils.validateConf()) {
      LOG.error("Invalid configuration found");
      System.exit(-1);
    }

    AlluxioWorkerService worker = AlluxioWorkerService.Factory.get();
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running Alluxio worker, stopping it and exiting.", e);
      try {
        worker.stop();
      } catch (Exception e2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping Alluxio worker, simply exiting.", e2);
      }
      System.exit(-1);
    }
  }

  private AlluxioWorker() {} // Not intended for instantiation
}
