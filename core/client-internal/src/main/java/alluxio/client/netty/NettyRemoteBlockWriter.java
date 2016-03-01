/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.netty;

import alluxio.Constants;
import alluxio.client.RemoteBlockWriter;
import alluxio.exception.ExceptionMessage;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Write data to a remote data server using Netty.
 */
@NotThreadSafe
public final class NettyRemoteBlockWriter implements RemoteBlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final Bootstrap mClientBootstrap;
  private final ClientHandler mHandler;

  private boolean mOpen;
  private InetSocketAddress mAddress;
  private long mBlockId;
  private long mSessionId;

  // Total number of bytes written to the remote block.
  private long mWrittenBytes;

  /**
   * Creates a new {@link NettyRemoteBlockWriter}.
   */
  public NettyRemoteBlockWriter() {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
    mOpen = false;
  }

  @Override
  public void open(InetSocketAddress address, long blockId, long sessionId) throws IOException {
    if (mOpen) {
      throw new IOException(
          ExceptionMessage.WRITER_ALREADY_OPEN.getMessage(mAddress, mBlockId, mSessionId));
    }
    mAddress = address;
    mBlockId = blockId;
    mSessionId = sessionId;
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
      // TODO(hy): keep connection open across multiple write calls.
      ChannelFuture f = mClientBootstrap.connect(mAddress).sync();

      LOG.info("Connected to remote machine {}", mAddress);
      Channel channel = f.channel();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCBlockWriteRequest(mSessionId, mBlockId, mWrittenBytes, length,
          new DataByteArrayChannel(bytes, offset, length)));

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      channel.close().sync();

      switch (response.getType()) {
        case RPC_BLOCK_WRITE_RESPONSE:
          RPCBlockWriteResponse resp = (RPCBlockWriteResponse) response;
          RPCResponse.Status status = resp.getStatus();
          LOG.info("status: {} from remote machine {} received", status, mAddress);

          if (status != RPCResponse.Status.SUCCESS) {
            throw new IOException(ExceptionMessage.BLOCK_WRITE_ERROR.getMessage(mBlockId,
                mSessionId, mAddress, status.getMessage()));
          }
          mWrittenBytes += length;
          break;
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
              .getMessage(response.getType(), RPCMessage.Type.RPC_BLOCK_WRITE_RESPONSE));
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      mHandler.removeListener(listener);
    }
  }
}
