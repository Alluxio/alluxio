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

import alluxio.Constants;
import alluxio.ProcessUtils;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Entry point for the Alluxio proxy program.
 */
@NotThreadSafe
public final class AlluxioProxy {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioProxy.class);

  /**
   * Starts the Alluxio proxy.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    if (args.length != 0) {
      LOG.info("java -cp {} {}", RuntimeConstants.ALLUXIO_JAR,
          AlluxioProxy.class.getCanonicalName());
      System.exit(-1);
    }

    if (!ConfigurationUtils.masterHostConfigured()) {
      ProcessUtils.fatalError(LOG,
          "Cannot run alluxio proxy; master hostname is not "
              + "configured. Please modify %s to either set %s or configure zookeeper with "
              + "%s=true and %s=[comma-separated zookeeper master addresses]",
          Constants.SITE_PROPERTIES, PropertyKey.MASTER_HOSTNAME.toString(),
          PropertyKey.ZOOKEEPER_ENABLED.toString(), PropertyKey.ZOOKEEPER_ADDRESS.toString());
    }

    CommonUtils.PROCESS_TYPE.set(CommonUtils.ProcessType.PROXY);
    ProxyProcess process = ProxyProcess.Factory.create();
    ProcessUtils.run(process);
  }

  private AlluxioProxy() {} // prevent instantiation
}
