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

/**
 * Created through {@link WebServerSimpleService.Factory}.
 * This service differs from {@link PrimaryOnlyWebServerSimpleService} because it deploys a web
 * server after being started. It stops said web server after being stopped.
 */
class AlwaysOnWebServerSimpleService extends WebServerSimpleService {
  AlwaysOnWebServerSimpleService(MasterProcess masterProcess) {
    super(masterProcess);
  }

  @Override
  public synchronized void start() {
    startWebServer();
  }

  @Override
  public synchronized void promote() {}

  @Override
  public synchronized void demote() {}

  @Override
  public synchronized void stop() {
    stopWebServer();
  }
}
