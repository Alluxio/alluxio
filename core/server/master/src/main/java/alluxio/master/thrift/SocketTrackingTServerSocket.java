package alluxio.master.thrift;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;

import com.google.common.io.Closer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extension of TServerSocket which tracks all accepted sockets and closes them when the server
 * socket is closed.
 */
public class SocketTrackingTServerSocket extends TServerSocket {
  private static final Logger LOG = LoggerFactory.getLogger(SocketTrackingTServerSocket.class);
  private static final long CLEANUP_INTERVAL_MS =
      Configuration.getMs(PropertyKey.MASTER_CLIENT_SOCKET_CLEANUP_INTERVAL);

  private final Set<Socket> mSockets = ConcurrentHashMap.newKeySet();
  private final Thread mCleanupThread;

  /**
   * @param bindAddr bind address for the socket
   * @param clientTimeout timeout for client sockets from accept
   */
  public SocketTrackingTServerSocket(InetSocketAddress bindAddr, int clientTimeout)
      throws TTransportException {
    super(bindAddr, clientTimeout);
    mCleanupThread = new Thread(this::removeClosedSockets);
    mCleanupThread.start();
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
    mCleanupThread.interrupt();
    try {
      mCleanupThread.join(Constants.SECOND_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return;
    }
    if (mCleanupThread.isAlive()) {
      LOG.warn("Failed to stop socket cleanup thread.");
    }
  }

  /**
   * Closes all socket connections that have been accepted by this server socket.
   */
  private void closeClientSockets() throws IOException {
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
    while (!Thread.interrupted()) {
      try {
        Thread.sleep(CLEANUP_INTERVAL_MS);
      } catch (InterruptedException e) {
        return;
      }
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
}
