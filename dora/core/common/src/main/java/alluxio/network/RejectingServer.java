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

import alluxio.Constants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

/**
 * A server which listens on a socket and closes all connections. This is useful for sitting on a
 * socket without causing clients to hang when they try to connect.
 */
public final class RejectingServer extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(RejectingServer.class);

  private final InetSocketAddress mAddress;
  private ServerSocket mServerSocket;

  /**
   * @param address the socket address to reject requests on
   */
  public RejectingServer(InetSocketAddress address) {
    super("RejectingServer-" + address);
    mAddress = address;
  }

  @Override
  public void run() {
    try {
      mServerSocket = new ServerSocket();
      mServerSocket.bind(mAddress);
      mServerSocket.setReuseAddress(true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    while (!Thread.interrupted()) {
      try {
        Socket s = mServerSocket.accept();
        s.close();
      } catch (SocketException e) {
        return;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Stops the server and joins the server thread.
   */
  public void stopAndJoin() {
    interrupt();
    if (mServerSocket != null) {
      try {
        mServerSocket.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      join(5L * Constants.SECOND_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (isAlive()) {
      LOG.warn("Failed to stop rejecting server thread");
    }
  }
}
