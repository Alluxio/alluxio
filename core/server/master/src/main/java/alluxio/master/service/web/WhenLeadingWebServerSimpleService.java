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
import alluxio.master.service.rpc.RpcServerSimpleService;
import alluxio.network.RejectingServer;

import java.net.InetSocketAddress;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

class WhenLeadingWebServerSimpleService extends WebServerSimpleService {
  private final InetSocketAddress mBindAddress;
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingServer = null;

  WhenLeadingWebServerSimpleService(InetSocketAddress bindAddress, MasterProcess masterProcess) {
    super(masterProcess);
    mBindAddress = bindAddress;
  }

  @Override
  public synchronized void start() {
    mRejectingServer = new RejectingServer(mBindAddress);
    mRejectingServer.start();
    waitForBound();
  }

  @Override
  public synchronized void promote() {
    stopRejectingServer();
    waitForFree();
    startWebServer();
  }

  @Override
  public synchronized void demote() {
    stopWebServer();
    waitForFree();
    start(); // start rejecting server again
  }

  @Override
  public synchronized void stop() {
    stopWebServer();
    stopRejectingServer();
  }

  private synchronized void stopRejectingServer() {
    if (mRejectingServer != null) {
      mRejectingServer.stopAndJoin();
      mRejectingServer = null;
    }
  }

  private void waitForFree() {
    RpcServerSimpleService.waitFor(false, mBindAddress);
  }

  private void waitForBound() {
    RpcServerSimpleService.waitFor(true, mBindAddress);
  }
}
