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

package alluxio.network.thrift;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.io.Closer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Extension of TServerSocket which tracks all accepted sockets and closes them when the server
 * socket is closed.
 */
public class SocketTrackingTServerSocket extends TServerSocket {
  private static final Logger LOG = LoggerFactory.getLogger(SocketTrackingTServerSocket.class);
  private static final long CLEANUP_INTERVAL_MS =
      Configuration.getMs(PropertyKey.MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL);

  private final Set<Socket> mSockets = ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService mExecutor;

  /**
   * @param args the arguments for creating the server socket
   */
  public SocketTrackingTServerSocket(ServerSocketTransportArgs args) throws TTransportException {
    super(args);
    mExecutor = Executors
        .newSingleThreadScheduledExecutor(ThreadFactoryUtils.build("socket-closer-thread", true));
    mExecutor.scheduleAtFixedRate(this::removeClosedSockets, CLEANUP_INTERVAL_MS,
        CLEANUP_INTERVAL_MS, TimeUnit.MILLISECONDS);
  }

  @Override
  public TSocket acceptImpl() throws TTransportException {
    TSocket socket = super.acceptImpl();
    mSockets.add(socket.getSocket());
    return socket;
  }

  @Override
  public void close() {
    super.close();
    try {
      closeClientSockets();
    } catch (IOException e) {
      LOG.error("Could not close client sockets", e);
    }
    shutdownExecutor();
  }

  /**
   * Shuts down the executor service.
   */
  private void shutdownExecutor() {
    // Possible since super constructor can call close().
    if (mExecutor == null) {
      return;
    }
    mExecutor.shutdownNow();
    try {
      if (!mExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
        LOG.warn("Failed to stop socket cleanup thread.");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
  }

  /**
   * Closes all socket connections that have been accepted by this server socket.
   */
  private void closeClientSockets() throws IOException {
    // Possible since super constructor can call close().
    if (mSockets == null) {
      return;
    }
    Closer closer = Closer.create();
    int count = 0;
    for (Socket s : mSockets) {
      if (!s.isClosed()) {
        closer.register(s);
        count++;
      }
    }
    closer.close();
    LOG.info("Closed {} client sockets", count);
  }

  /**
   * Periodically clean up any closed sockets.
   */
  private void removeClosedSockets() {
    // This is best-effort, and may not remove sockets added to the mSockets set after the
    // iterator was created. Those sockets will be checked on the next sweep.
    for (Iterator<Socket> it = mSockets.iterator(); it.hasNext();) {
      Socket s = it.next();
      if (s.isClosed()) {
        it.remove();
      }
    }
  }
}
