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

package alluxio;

import alluxio.master.MasterFactory;
import alluxio.worker.WorkerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Utility methods for running server binaries.
 */
public final class ServerUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static ServiceLoader<MasterFactory> sMasterServiceLoader;
  private static List<String> sMasterServiceNames;
  private static ServiceLoader<WorkerFactory> sWorkerServiceLoader;

  /**
   * Runs the given server.
   *
   * @param server the server to run
   * @param name the server name
   */
  public static void run(Server server, String name) {
    try {
      server.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running {}, stopping it and exiting.", name, e);
      try {
        server.stop();
      } catch (Exception e2) {
        // continue to exit
        LOG.error("Uncaught exception while stopping {}, simply exiting.", name, e2);
      }
      System.exit(-1);
    }
  }

  /**
   * @return service loader for master factories
   */
  public static synchronized ServiceLoader<MasterFactory> getMasterServiceLoader() {
    if (sMasterServiceLoader == null) {
      sMasterServiceLoader =
          ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
    }
    return sMasterServiceLoader;

  }

  /**
   * @return the list of master service names
   */
  public static List<String> getMasterServiceNames() {
    if (sMasterServiceNames == null) {
      sMasterServiceNames = new ArrayList<>();
      sMasterServiceNames.add(Constants.BLOCK_MASTER_NAME);
      sMasterServiceNames.add(Constants.FILE_SYSTEM_MASTER_NAME);
      sMasterServiceNames.add(Constants.LINEAGE_MASTER_NAME);
      for (MasterFactory factory : getMasterServiceLoader()) {
        if (factory.isEnabled()) {
          sMasterServiceNames.add(factory.getName());
        }
      }
    }
    return sMasterServiceNames;
  }

  /**
   * @return service loader for worker factories
   */
  public static synchronized ServiceLoader<WorkerFactory> getWorkerServiceLoader() {
    if (sWorkerServiceLoader == null) {
      sWorkerServiceLoader =
          ServiceLoader.load(WorkerFactory.class, WorkerFactory.class.getClassLoader());
    }
    return sWorkerServiceLoader;
  }

  private ServerUtils() {} // prevent instantiation
}
