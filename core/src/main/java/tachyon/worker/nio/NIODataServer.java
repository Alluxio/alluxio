package tachyon.worker.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.google.common.io.Closeables;

import tachyon.Constants;
import tachyon.conf.CommonConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;

/**
 * The Server to serve data file read request from remote machines. The current implementation
 * is based on non-blocking NIO.
 */
public class NIODataServer implements Runnable, DataServer {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  // The host:port combination to listen on
  private InetSocketAddress mAddress;

  // The channel on which we will accept connections
  private ServerSocketChannel mServerChannel;

  // The selector we will be monitoring.
  private Selector mSelector;

  private Map<SocketChannel, DataServerMessage> mSendingData = Collections
      .synchronizedMap(new HashMap<SocketChannel, DataServerMessage>());
  private Map<SocketChannel, DataServerMessage> mReceivingData = Collections
      .synchronizedMap(new HashMap<SocketChannel, DataServerMessage>());

  // The blocks locker manager.
  private final BlocksLocker mBlockLocker;
  private final Thread mListenerThread;

  private volatile boolean mShutdown = false;
  private volatile boolean mShutdowned = false;

  /**
   * Create a data server with direct access to worker storage.
   * 
   * @param address
   *          The address of the data server.
   * @param locker
   *          The lock system for lock blocks.
   */
  public NIODataServer(InetSocketAddress address, BlocksLocker locker) {
    LOG.info("Starting DataServer @ " + address);
    CommonConf.assertValidPort(address);
    mAddress = address;
    mBlockLocker = locker;
    try {
      mSelector = initSelector();
      mListenerThread = new Thread(this);
      mListenerThread.start();
    } catch (IOException e) {
      LOG.error(e.getMessage() + mAddress, e);
      throw Throwables.propagate(e);
    }
  }

  private void accept(SelectionKey key) throws IOException {
    // For an accept to be pending the channel must be a server socket channel
    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();

    // Accept the connection and make it non-blocking
    SocketChannel socketChannel = serverSocketChannel.accept();
    socketChannel.configureBlocking(false);

    // Register the new SocketChannel with our Selector, indicating we'd like to be notified
    // when there is data waiting to be read.
    socketChannel.register(mSelector, SelectionKey.OP_READ);
  }

  /**
   * Close the data server.
   * 
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    mShutdown = true;
    mServerChannel.close();
    mSelector.close();
  }

  /**
   * Gets the port listening on.
   */
  @Override
  public int getPort() {
    return mServerChannel.socket().getLocalPort();
  }

  private Selector initSelector() throws IOException {
    // Create a new selector
    Selector socketSelector = SelectorProvider.provider().openSelector();

    // Create a new non-blocking server socket channel
    try {
      mServerChannel = ServerSocketChannel.open();
      mServerChannel.configureBlocking(false);

      // Bind the server socket to the specified address and port
      mServerChannel.socket().bind(mAddress);

      // Register the server socket channel, indicating an interest in accepting new connections.
      mServerChannel.register(socketSelector, SelectionKey.OP_ACCEPT);

      return socketSelector;
    } catch (IOException e) {
      // we wan't to throw the original IO issue, not the close issue, so don't throw
      // #close IOException.
      socketSelector.close();
      throw e;
    } catch (RuntimeException e) {
      // we wan't to throw the original IO issue, not the close issue, so don't throw
      // #close IOException.
      socketSelector.close();
      throw e;
    }
  }

  /**
   * @return true if the server is closed, false otherwise
   */
  @Override
  public boolean isClosed() {
    return mShutdowned;
  }

  private void read(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();

    DataServerMessage tMessage;
    if (mReceivingData.containsKey(socketChannel)) {
      tMessage = mReceivingData.get(socketChannel);
    } else {
      tMessage = DataServerMessage.createBlockRequestMessage();
      mReceivingData.put(socketChannel, tMessage);
    }

    // Attempt to read off the channel
    int numRead;
    try {
      numRead = tMessage.recv(socketChannel);
    } catch (IOException e) {
      // The remote forcibly closed the connection, cancel the selection key and close the channel.
      key.cancel();
      socketChannel.close();
      mReceivingData.remove(socketChannel);
      mSendingData.remove(socketChannel);
      return;
    }

    if (numRead == -1) {
      // Remote entity shut the socket down cleanly. Do the same from our end and cancel the
      // channel.
      key.channel().close();
      key.cancel();
      mReceivingData.remove(socketChannel);
      mSendingData.remove(socketChannel);
      return;
    }

    if (tMessage.isMessageReady()) {
      if (tMessage.getBlockId() <= 0) {
        LOG.error("Invalid block id " + tMessage.getBlockId());
        return;
      }

      key.interestOps(SelectionKey.OP_WRITE);
      LOG.info("Get request for " + tMessage.getBlockId());
      int lockId = mBlockLocker.lock(tMessage.getBlockId());
      DataServerMessage tResponseMessage =
          DataServerMessage.createBlockResponseMessage(true, tMessage.getBlockId(),
              tMessage.getOffset(), tMessage.getLength());
      tResponseMessage.setLockId(lockId);
      mSendingData.put(socketChannel, tResponseMessage);
    }
  }

  @Override
  public void run() {
    while (!mShutdown) {
      try {
        // Wait for an event one of the registered channels.
        mSelector.select();
        if (mShutdown) {
          break;
        }

        // Iterate over the set of keys for which events are available
        Iterator<SelectionKey> selectKeys = mSelector.selectedKeys().iterator();
        while (selectKeys.hasNext()) {
          SelectionKey key = selectKeys.next();
          selectKeys.remove();

          if (!key.isValid()) {
            continue;
          }

          // Check what event is available and deal with it.
          // TODO These should be multi-thread.
          if (key.isAcceptable()) {
            accept(key);
          } else if (key.isReadable()) {
            read(key);
          } else if (key.isWritable()) {
            write(key);
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        if (mShutdown) {
          break;
        }
        throw new RuntimeException(e);
      }
    }
    mShutdowned = true;
  }

  private void write(SelectionKey key) {
    SocketChannel socketChannel = (SocketChannel) key.channel();

    DataServerMessage sendMessage = mSendingData.get(socketChannel);

    boolean closeChannel = false;
    try {
      sendMessage.send(socketChannel);
    } catch (IOException e) {
      closeChannel = true;
      LOG.error(e.getMessage());
    }

    if (sendMessage.finishSending() || closeChannel) {
      try {
        key.channel().close();
      } catch (IOException e) {
        LOG.error(e.getMessage());
      }
      key.cancel();
      mReceivingData.remove(socketChannel);
      mSendingData.remove(socketChannel);
      sendMessage.close();
      mBlockLocker.unlock(Math.abs(sendMessage.getBlockId()), sendMessage.getLockId());
    }
  }
}