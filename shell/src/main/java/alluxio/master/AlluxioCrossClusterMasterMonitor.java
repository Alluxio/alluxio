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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alluxio cross cluster master monitor for inquiring the AlluxioCrossClusterMaster
 * service availability.
 */
public final class AlluxioCrossClusterMasterMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioCrossClusterMasterMonitor.class);

  /**
   * Starts the Alluxio master monitor.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.warn("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioCrossClusterMasterMonitor.class.getCanonicalName());
      LOG.warn("ignoring arguments");
    }

    AlluxioConfiguration alluxioConf = Configuration.global();

    MasterHealthCheckClient.Builder builder = new MasterHealthCheckClient.Builder(alluxioConf);
    builder.withAlluxioMasterType(MasterHealthCheckClient.MasterType.CROSS_CLUSTER_MASTER);
    builder.withProcessCheck(ConfigurationUtils.isHaMode(alluxioConf));
    HealthCheckClient client = builder.build();
    if (!client.isServing()) {
      System.exit(1);
    }
    System.exit(0);
  }

  private AlluxioCrossClusterMasterMonitor() {} // prevent instantiation
}
