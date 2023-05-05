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

package alluxio.master.service.web;

import alluxio.master.MasterProcess;
import alluxio.master.service.rpc.RpcServerService;
import alluxio.network.RejectingServer;

import com.google.common.base.Preconditions;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Created through {@link WebServerService.Factory}.
 * This service differs from {@link AlwaysOnWebServerService} because it deploys a web
 * server only after being promoted. It stops said web server after being demoted or stopped.
 * When a web server is not deployed, a rejecting server is deployed instead (after the service
 * has been started).
 */
class PrimaryOnlyWebServerService extends WebServerService {
  private final InetSocketAddress mBindAddress;
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingServer = null;

  PrimaryOnlyWebServerService(InetSocketAddress bindAddress, MasterProcess masterProcess) {
    super(masterProcess);
    mBindAddress = bindAddress;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    startRejectingServer();
  }

  @Override
  public synchronized void promote() {
    LOG.info("Promoting {}", this.getClass().getSimpleName());
    stopRejectingServer();
    waitForFree();
    startWebServer();
  }

  @Override
  public synchronized void demote() {
    LOG.info("Demoting {}", this.getClass().getSimpleName());
    stopWebServer();
    waitForFree();
    startRejectingServer();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}", this.getClass().getSimpleName());
    stopWebServer();
    stopRejectingServer();
  }

  private synchronized void startRejectingServer() {
    Preconditions.checkState(mRejectingServer == null, "rejecting server must not be running");
    mRejectingServer = new RejectingServer(mBindAddress);
    mRejectingServer.start();
    waitForBound();
  }

  private synchronized void stopRejectingServer() {
    if (mRejectingServer != null) {
      mRejectingServer.stopAndJoin();
      mRejectingServer = null;
    }
  }

  private void waitForFree() {
    RpcServerService.waitFor(false, mBindAddress);
  }

  private void waitForBound() {
    RpcServerService.waitFor(true, mBindAddress);
  }
}
