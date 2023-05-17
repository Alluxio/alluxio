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

package alluxio.master.service.snapshotrpc.rpc;

import alluxio.master.AlluxioMasterProcess;
import alluxio.master.Master;
import alluxio.master.MasterRegistry;

import java.net.InetSocketAddress;

/**
 * Created by {@link Factory}.
 */
class AlwaysOnSnapshotRpcService extends SnapshotRpcServerService {

  protected AlwaysOnSnapshotRpcService(InetSocketAddress bindAddress,
      AlluxioMasterProcess masterProcess, MasterRegistry masterRegistry) {
    super(bindAddress, masterProcess, masterRegistry);
  }

  @Override
  public void start() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    startGrpcServer(Master::getJournalServices);
  }

  @Override
  public void promote() {}

  @Override
  public void demote() {}

  @Override
  public void stop() {
    LOG.info("Stopping {}", this.getClass().getSimpleName());
    stopGrpcServer();
    stopRpcExecutor();
  }
}
