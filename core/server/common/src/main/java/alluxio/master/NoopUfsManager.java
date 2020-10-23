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

import alluxio.conf.ServerConfiguration;
import alluxio.underfs.AbstractUfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.network.NetworkAddressUtils;

import java.io.IOException;

/**
 * Implementation of UfsManager that does nothing. This is useful for testing and
 * for situations where we don't want to start a real UfsManager,e.g. when formatting the journal.
 */
public class NoopUfsManager extends AbstractUfsManager {

  @Override
  protected void connectUfs(UnderFileSystem fs) throws IOException {
    fs.connectFromMaster(
        NetworkAddressUtils.getConnectHost(NetworkAddressUtils.ServiceType.MASTER_RPC,
            ServerConfiguration.global()));
  }
}
