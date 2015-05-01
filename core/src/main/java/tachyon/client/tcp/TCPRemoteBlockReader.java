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

package tachyon.client.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.client.RemoteBlockReader;
import tachyon.worker.nio.DataServerMessage;

/**
 * Read data from remote data server using TCP.
 */
public final class TCPRemoteBlockReader implements RemoteBlockReader {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  @Override
  public ByteBuffer readRemoteBlock(String host, int port, long blockId, long offset, long length)
      throws IOException {
    InetSocketAddress address = new InetSocketAddress(host, port);
    SocketChannel socketChannel = SocketChannel.open();
    try {
      socketChannel.connect(address);

      LOG.info("Connected to remote machine " + address);
      DataServerMessage sendMsg =
          DataServerMessage.createBlockRequestMessage(blockId, offset, length);
      while (!sendMsg.finishSending()) {
        sendMsg.send(socketChannel);
      }

      LOG.info("Data " + blockId + " to remote machine " + address + " sent");

      DataServerMessage recvMsg =
          DataServerMessage.createBlockResponseMessage(false, blockId, null);
      while (!recvMsg.isMessageReady()) {
        int numRead = recvMsg.recv(socketChannel);
        if (numRead == -1) {
          LOG.warn("Read nothing");
        }
      }
      LOG.info("Data " + blockId + " from remote machine " + address + " received");

      if (recvMsg.getBlockId() < 0) {
        LOG.info("Data " + recvMsg.getBlockId() + " is not in remote machine.");
        return null;
      }

      return recvMsg.getReadOnlyData();
    } finally {
      socketChannel.close();
    }
  }
}
