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

import alluxio.HealthCheckClient;
import alluxio.RuntimeConstants;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alluxio worker monitor for inquiring AlluxioWorker service availability.
 */
public final class AlluxioWorkerMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorkerMonitor.class);

  /**
   * Starts the Alluxio worker monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioWorkerMonitor.class.getCanonicalName());
      LOG.warn("ignoring arguments");
    }

    HealthCheckClient client = new WorkerHealthCheckClient(
        NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.WORKER_RPC),
        () -> new ExponentialBackoffRetry(50, 100, 2));
    if (!client.isServing()) {
      System.exit(1);
    }
    System.exit(0);
  }

  private AlluxioWorkerMonitor() {} // prevent instantiation
}
