/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.worker.nio;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.DataServer;
import alluxio.worker.DataServerMessage;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The server to serve data file read requests from remote machines. The current implementation is
 * based on non-blocking NIO.
 */
@NotThreadSafe
public final class NIODataServer implements Runnable, DataServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // The host:port combination to listen on
  private final InetSocketAddress mAddress;

  // The channel on which we will accept connections
  private ServerSocketChannel mServerChannel;

  // The selector we will be monitoring.
  private Selector mSelector;

  private final Map<SocketChannel, DataServerMessage> mSendingData =
      Collections.synchronizedMap(new HashMap<SocketChannel, DataServerMessage>());
  private final Map<SocketChannel, DataServerMessage> mReceivingData =
      Collections.synchronizedMap(new HashMap<SocketChannel, DataServerMessage>());

  // The BlockWorker handler
  private final BlockWorker mBlockWorker;
  private final Thread mListenerThread;

  private volatile boolean mShutdown = false;
  private volatile boolean mShutdownComplete = false;

  /**
   * Creates a data server with direct access to worker storage.
   *
   * @param address the address of the data server
   * @param blockWorker the lock system for lock blocks
   * @param configuration Alluxio configuration
   */
  public NIODataServer(final InetSocketAddress address, final BlockWorker blockWorker,
      Configuration configuration) {
    LOG.info("Starting DataServer @ {}", address);
    NetworkAddressUtils.assertValidPort(Preconditions.checkNotNull(address));
    mAddress = address;
    mBlockWorker = Preconditions.checkNotNull(blockWorker);
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

    // Register the new SocketChannel with our Selector, indicating that we'd like to be notified
    // when there is data waiting to be read.
    socketChannel.register(mSelector, SelectionKey.OP_READ);
  }

  @Override
  public void close() throws IOException {
    mShutdown = true;
    mServerChannel.close();
    mSelector.close();
  }

  @Override
  public String getBindHost() {
    return mServerChannel.socket().getInetAddress().getHostAddress();
  }

  @Override
  public int getPort() {
    return mServerChannel.socket().getLocalPort();
  }

  @Override
  public boolean isClosed() {
    return mShutdownComplete;
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
      // we want to throw the original IO issue, not the close issue, so don't throw
      // #close IOException.
      try {
        socketSelector.close();
      } catch (IOException ex) {
        // ignore, we want the other exception
        LOG.warn("Unable to close socket selector. Exception: {}", ex.getMessage());
      }
      throw e;
    } catch (RuntimeException e) {
      // we want to throw the original IO issue, not the close issue, so don't throw
      // #close IOException.
      try {
        socketSelector.close();
      } catch (IOException ex) {
        // ignore, we want the other exception
        LOG.warn("Unable to close socket selector. Exception: {}", ex.getMessage());
      }
      throw e;
    }
  }

  private void read(SelectionKey key) throws Exception {
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
        LOG.error("Invalid block id {}", tMessage.getBlockId());
        return;
      }

      key.interestOps(SelectionKey.OP_WRITE);
      final long blockId = tMessage.getBlockId();
      LOG.info("Get request for blockId: {}", blockId);

      long lockId = tMessage.getLockId();
      long sessionId = tMessage.getSessionId();
      BlockReader reader =
          mBlockWorker.readBlockRemote(sessionId, blockId, lockId);
      ByteBuffer data;
      int dataLen = 0;
      try {
        data = reader.read(tMessage.getOffset(), tMessage.getLength());
        mBlockWorker.accessBlock(sessionId, blockId);
        dataLen = data.limit();
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        data = null;
      } finally {
        reader.close();
      }
      DataServerMessage tResponseMessage = DataServerMessage.createBlockResponseMessage(true,
          blockId, tMessage.getOffset(), dataLen, data);
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
          // TODO(dcapwell): These should be multi-thread.
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
        // Close server before exiting loop.
        try {
          close();
        } catch (Exception e2) {
          LOG.error("Exception when closing data server. message: {}", e2.getMessage());
        }
        // Mark the server as shut down.
        mShutdownComplete = true;
        throw new RuntimeException(e);
      }
    }
    mShutdownComplete = true;
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
    }
  }
}
