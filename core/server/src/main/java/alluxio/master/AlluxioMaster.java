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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Entry point for Alluxio master.
 */
@NotThreadSafe
public final class AlluxioMaster {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The master services' names. */
  private static List<String> sServiceNames;

  /** The master service loaders. */
  private static ServiceLoader<MasterFactory> sServiceLoader;

  /**
   * @return the (cached) master service loader
   */
  protected static ServiceLoader<MasterFactory> getServiceLoader() {
    if (sServiceLoader != null) {
      return sServiceLoader;
    }
    // Discover and register the available factories.
    // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader.
    sServiceLoader = ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
    return sServiceLoader;
  }

  /**
   * @return the (cached) list of the enabled master services' names
   */
  public static List<String> getServiceNames() {
    if (sServiceNames != null) {
      return sServiceNames;
    }
    sServiceNames = new ArrayList<>();
    sServiceNames.add(Constants.BLOCK_MASTER_NAME);
    sServiceNames.add(Constants.FILE_SYSTEM_MASTER_NAME);
    sServiceNames.add(Constants.LINEAGE_MASTER_NAME);

    for (MasterFactory factory : getServiceLoader()) {
      if (factory.isEnabled()) {
        sServiceNames.add(factory.getName());
      }
    }

    return sServiceNames;
  }

  /**
   * Factory for creating {@link AlluxioMasterService}.
   */
  @ThreadSafe
  public static final class Factory {
    /**
     * @return a new instance of {@link AlluxioMasterService}
     */
    public static AlluxioMasterService create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioMaster();
      }
      return new DefaultAlluxioMaster();
    }

    private Factory() {} // prevent instantiation
  }

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

    AlluxioMasterService master = Factory.create();
    try {
      master.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running Alluxio master, stopping it and exiting.", e);
      try {
        master.stop();
      } catch (Exception e2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping Alluxio master, simply exiting.", e2);
      }
      System.exit(-1);
    }
  }

  private AlluxioMaster() {} // prevent instantiation
}
