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

import alluxio.master.AlluxioMasterProcess;

import java.net.InetSocketAddress;

class WhenLeadingWebServerSimpleService extends WebServerSimpleService {

  WhenLeadingWebServerSimpleService(InetSocketAddress bindAddress,
      AlluxioMasterProcess masterProcess) {
    super(bindAddress, masterProcess);
  }

  @Override
  public void start() {
    startRejectingServer();
  }

  @Override
  public void promote() {
    stopRejectingServer();
    startWebServer();
  }

  @Override
  public void demote() {
    stopWebServer();
    startRejectingServer();
  }

  @Override
  public void stop() {
    stopWebServer();
    stopRejectingServer();
  }
}
