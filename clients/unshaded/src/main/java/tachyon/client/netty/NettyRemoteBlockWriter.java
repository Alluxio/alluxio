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

package tachyon.client.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import tachyon.Constants;
import tachyon.client.RemoteBlockWriter;
import tachyon.network.protocol.RPCBlockWriteRequest;
import tachyon.network.protocol.RPCBlockWriteResponse;
import tachyon.network.protocol.RPCErrorResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCResponse;
import tachyon.network.protocol.databuffer.DataByteArrayChannel;

/**
 * Write data to a remote data server using Netty.
 */
public final class NettyRemoteBlockWriter implements RemoteBlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Bootstrap mClientBootstrap;
  private final ClientHandler mHandler;

  private boolean mOpen;
  private InetSocketAddress mAddress;
  private long mBlockId;
  private long mUserId;

  // Total number of bytes written to the remote block.
  private long mWrittenBytes;

  public NettyRemoteBlockWriter() {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
    mOpen = false;
  }

  @Override
  public void open(InetSocketAddress address, long blockId, long userId) throws IOException {
    if (mOpen) {
      throw new IOException("This writer is already open for address: " + mAddress + ", blockId: "
          + mBlockId + ", userId: " + mUserId);
    }
    mAddress = address;
    mBlockId = blockId;
    mUserId = userId;
    mWrittenBytes = 0;
    mOpen = true;
  }

  @Override
  public void close() {
    if (mOpen) {
      mOpen = false;
    }
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    SingleResponseListener listener = new SingleResponseListener();
    try {
      // TODO: keep connection open across multiple write calls.
      ChannelFuture f = mClientBootstrap.connect(mAddress).sync();

      LOG.info("Connected to remote machine " + mAddress);
      Channel channel = f.channel();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCBlockWriteRequest(mUserId, mBlockId, mWrittenBytes, length,
          new DataByteArrayChannel(bytes, offset, length)));

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      channel.close().sync();

      switch (response.getType()) {
        case RPC_BLOCK_WRITE_RESPONSE:
          RPCBlockWriteResponse resp = (RPCBlockWriteResponse) response;
          RPCResponse.Status status = resp.getStatus();
          LOG.info("status: {} from remote machine {} received", status, mAddress);

          if (status != RPCResponse.Status.SUCCESS) {
            throw new IOException("error writing blockId: " + mBlockId + ", userId: " + mUserId
                + ", address: " + mAddress + ", message: " + status.getMessage());
          }
          mWrittenBytes += length;
          break;
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException("Unexpected response message type: " + response.getType()
              + " (expected: " + RPCMessage.Type.RPC_BLOCK_WRITE_RESPONSE + ")");
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      mHandler.removeListener(listener);
    }
  }
}
