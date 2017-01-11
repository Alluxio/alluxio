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
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.ServerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for the Alluxio worker.
 */
@ThreadSafe
public final class AlluxioWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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

    if (!Configuration.containsKey(PropertyKey.MASTER_HOSTNAME)) {
      System.out.println("Cannot start worker; master hostname is not configured. Please set "
          + PropertyKey.MASTER_HOSTNAME.toString() + " in alluxio-site.properties.");
      System.exit(1);
    }

    AlluxioWorkerService worker = AlluxioWorkerService.Factory.create();
    ServerUtils.run(worker, "Alluxio worker");
  }

  private AlluxioWorker() {} // prevent instantiation
}
