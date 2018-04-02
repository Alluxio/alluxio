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

import alluxio.HealthCheckClient;
import alluxio.RuntimeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alluxio master monitor for inquiring the AlluxioMaster service availability.
 */
public final class AlluxioMasterMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioMasterMonitor.class);

  /**
   * Starts the Alluxio master monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.warn("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioMasterMonitor.class.getCanonicalName());
      LOG.warn("ignoring arguments");
    }

    HealthCheckClient client = new MasterHealthCheckClient.Builder().build();
    if (!client.isServing()) {
      System.exit(1);
    }
    System.exit(0);
  }

  private AlluxioMasterMonitor() {} // prevent instantiation
}
