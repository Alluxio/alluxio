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

package alluxio.csi.utils;

import io.netty.channel.unix.DomainSocketAddress;

import java.io.File;
import java.net.SocketAddress;

/**
 * Helper classes for gRPC utility functions.
 */
public final class GrpcHelper {

  protected static final String UNIX_DOMAIN_SOCKET_PREFIX = "unix://";

  private GrpcHelper() {
    // hide constructor for utility class
  }

  public static SocketAddress getSocketAddress(String value) {
    if (value.startsWith(UNIX_DOMAIN_SOCKET_PREFIX)) {
      String filePath = value.substring(UNIX_DOMAIN_SOCKET_PREFIX.length());
      File file = new File(filePath);
      if (!file.isAbsolute()) {
        throw new IllegalArgumentException(
            "Unix domain socket file path must be absolute, file: " + value);
      }
      // Create the SocketAddress referencing the file.
      return new DomainSocketAddress(file);
    } else {
      throw new IllegalArgumentException("Given address " + value
          + " is not a valid unix domain socket path");
    }
  }
}
