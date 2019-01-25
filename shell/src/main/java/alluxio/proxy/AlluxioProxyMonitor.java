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

package alluxio.proxy;

import alluxio.HealthCheckClient;
import alluxio.RuntimeConstants;
import alluxio.conf.InstancedConfiguration;
import alluxio.retry.ExponentialBackoffRetry;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NetworkAddressUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alluxio proxy monitor for inquiring the AlluxioProxy service availability.
 */
public final class AlluxioProxyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioProxyMonitor.class);

  /**
   * Starts the Alluxio proxy monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioProxyMonitor.class.getCanonicalName());
      LOG.warn("ignoring arguments");
    }

    HealthCheckClient client = new ProxyHealthCheckClient(
        NetworkAddressUtils.getBindAddress(NetworkAddressUtils.ServiceType.PROXY_WEB,
            new InstancedConfiguration(ConfigurationUtils.defaults())),
        () -> new ExponentialBackoffRetry(50, 100, 2));
    if (!client.isServing()) {
      System.exit(1);
    }
    System.exit(0);
  }

  private AlluxioProxyMonitor() {} // prevent instantiation
}
