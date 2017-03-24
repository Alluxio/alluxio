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
  private static final Logger LOG = LoggerFactory.getLogger(ServerUtils.class);

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
    return ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
  }

  /**
   * @return the list of master service names
   */
  public static List<String> getMasterServiceNames() {
    List<String> masterServiceNames = new ArrayList<>();
    for (MasterFactory factory : getMasterServiceLoader()) {
      if (factory.isEnabled()) {
        masterServiceNames.add(factory.getName());
      }
    }
    return masterServiceNames;
  }

  /**
   * @return service loader for worker factories
   */
  public static synchronized ServiceLoader<WorkerFactory> getWorkerServiceLoader() {
    return ServiceLoader.load(WorkerFactory.class, WorkerFactory.class.getClassLoader());
  }

  private ServerUtils() {} // prevent instantiation
}
