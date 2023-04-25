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

package alluxio.fuse;

import alluxio.web.WebServer;

import java.net.InetSocketAddress;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Fuse web server.
 */
@NotThreadSafe
public final class FuseWebServer extends WebServer {
  /**
   * Creates a new instance of {@link FuseWebServer}. It pairs URLs with servlets.
   *
   * @param serviceName name of the web service
   * @param address address of the server
   */
  public FuseWebServer(String serviceName, InetSocketAddress address) {
    super(serviceName, address);
  }
}
