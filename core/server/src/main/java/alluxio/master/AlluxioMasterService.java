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
import alluxio.Server;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.lineage.LineageMaster;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A master in the Alluxio system.
 */
public interface AlluxioMasterService extends Server {
  /**
   * Factory for creating {@link AlluxioMasterService}.
   */
  @ThreadSafe
  final class Factory {
    /** The master services' names. */
    private static List<String> sServiceNames;

    /** The master service loaders. */
    private static ServiceLoader<MasterFactory> sServiceLoader;

    /**
     * @return a new instance of {@link AlluxioMasterService}
     */
    public static AlluxioMasterService create() {
      if (Configuration.getBoolean(PropertyKey.ZOOKEEPER_ENABLED)) {
        return new FaultTolerantAlluxioMaster();
      }
      return new DefaultAlluxioMaster();
    }

    /**
     * @return the (cached) master service loader
     */
    static synchronized ServiceLoader<MasterFactory> getServiceLoader() {
      if (sServiceLoader != null) {
        return sServiceLoader;
      }
      // Discover and register the available factories.
      // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader.
      sServiceLoader =
          ServiceLoader.load(MasterFactory.class, MasterFactory.class.getClassLoader());
      return sServiceLoader;
    }

    /**
     * @return the (cached) list of the enabled master services' names
     */
    public static synchronized List<String> getServiceNames() {
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

    private Factory() {} // prevent instantiation
  }

  /**
   * @return the block master for this Alluxio master
   */
  BlockMaster getBlockMaster();

  /**
   * @return the file system master for this Alluxio master
   */
  FileSystemMaster getFileSystemMaster();

  /**
   * @return the lineage master for this Alluxio master
   */
  LineageMaster getLineageMaster();

  /**
   * @return this master's rpc address
   */
  InetSocketAddress getRpcAddress();

  /**
   * @return the start time of the master in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the master in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the master web service bind host (used by unit test only)
   */
  String getWebBindHost();

  /**
   * @return the master web service port (used by unit test only)
   */
  int getWebLocalPort();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();

  /**
   * Waits until the master is ready to server requests.
   */
  void waitForReady();
}
