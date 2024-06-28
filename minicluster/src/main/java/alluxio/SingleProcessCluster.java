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

import alluxio.master.AlluxioMasterProcess;
import alluxio.master.LocalAlluxioCluster;

/**
 * Single process cluster.
 */
public class SingleProcessCluster {

  /**
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    LocalAlluxioClusterResource mLocalAlluxioClusterResource =
        new LocalAlluxioClusterResource.Builder().build();
    mLocalAlluxioClusterResource.start();
    LocalAlluxioCluster cluster = mLocalAlluxioClusterResource.get();
    AlluxioMasterProcess master = cluster.getLocalAlluxioMaster().getMasterProcess();
    System.out.println(master.getWebAddress());
    System.out.println(master.getRpcAddress());
  }
}
