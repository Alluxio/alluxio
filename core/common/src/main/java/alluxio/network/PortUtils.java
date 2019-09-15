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

package alluxio.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A server which listens on a socket and closes all connections. This is useful for sitting on a
 * socket without causing clients to hang when they try to connect.
 */
public final class PortUtils {
  private static final Logger LOG = LoggerFactory.getLogger(PortUtils.class);

  private PortUtils() {} // prevent instantiation

  /**
   * @return a port that is currently free. This does not reserve the port, so the port may be taken
   *         by the time this method returns.
   */
  public static int getFreePort() {
    ServerSocket socket = null;

    // Passing ServerSocket port 0 sometimes returns a socket that has a conflicting port (by
    // another process). However, explicitly passing in a port number does not have the same issue.
    // Therefore, we must manually find a random port.
    for (int i = 0; i < 1000; i++) {
      try {
        socket = new ServerSocket(ThreadLocalRandom.current().nextInt(32768, 65535), 50, null);
        break;
      } catch (IOException e) {
        // retry
      }
    }

    if (socket == null) {
      throw new RuntimeException("Failed to find a free port");
    }

    int port;
    try {
      socket.setReuseAddress(true);
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return port;
  }
}
