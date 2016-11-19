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

import alluxio.Server;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.lineage.LineageMaster;

import java.net.InetSocketAddress;

/**
 * A worker in the Alluxio system.
 */
public interface AlluxioMasterService extends Server {
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
   * @return the start time of the worker in milliseconds
   */
  long getStartTimeMs();

  /**
   * @return the uptime of the worker in milliseconds
   */
  long getUptimeMs();

  /**
   * @return the worker web service bind host (used by unit test only)
   */
  String getWebBindHost();

  /**
   * @return the worker web service port (used by unit test only)
   */
  int getWebLocalPort();

  /**
   * @return true if the system is the leader (serving the rpc server), false otherwise
   */
  boolean isServing();

  /**
   * Waits until the worker is ready to server requests.
   */
  void waitForReady();
}
