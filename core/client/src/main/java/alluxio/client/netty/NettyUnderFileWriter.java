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
import alluxio.exception.ExceptionMessage;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCFileWriteResponse;
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
 * Writer for an under file system file through a worker data server via Netty. This class does
 * not keep lingering resources and does not need to be closed.
 */
@NotThreadSafe
public final class NettyUnderFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Netty bootstrap for the connection. */
  private final Bootstrap mClientBootstrap;
  /** Handler for Netty messages. */
  private final ClientHandler mHandler;
  /** Worker address. */
  private final InetSocketAddress mAddress;
  /** Worker specific file id of the file to write to. */
  private final long mUfsFileId;

  /** Total number of bytes written to the ufs file. */
  private long mWrittenBytes;

  /**
   * Constructor for a Netty based writer to an under file system file on a worker.
   *
   * @param address the address of the worker managing the file
   * @param tempUfsFileId the worker specific file id
   */
  public NettyUnderFileWriter(InetSocketAddress address, long tempUfsFileId) {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
    mAddress = address;
    mUfsFileId = tempUfsFileId;
    mWrittenBytes = 0;
  }

  /**
   * Writes data to the file in the under file system.
   *
   * @param bytes data to write
   * @param offset start offset of the data
   * @param length length to write
   * @throws IOException if an error occurs during the write
   */
  public void write(byte[] bytes, int offset, int length) throws IOException {
    SingleResponseListener listener = new SingleResponseListener();
    try {
      ChannelFuture f = mClientBootstrap.connect(mAddress).sync();

      LOG.info("Connected to remote machine {}", mAddress);
      Channel channel = f.channel();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCFileWriteRequest(mUfsFileId, mWrittenBytes, length,
          new DataByteArrayChannel(bytes, offset, length)));

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      channel.close().sync();

      switch (response.getType()) {
        case RPC_FILE_WRITE_RESPONSE:
          RPCFileWriteResponse resp = (RPCFileWriteResponse) response;
          RPCResponse.Status status = resp.getStatus();
          LOG.info("status: {} from remote machine {} received", status, mAddress);

          if (status != RPCResponse.Status.SUCCESS) {
            throw new IOException(ExceptionMessage.UNDER_FILE_WRITE_ERROR.getMessage(mUfsFileId,
                mAddress, status.getMessage()));
          }
          mWrittenBytes += length;
          break;
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
              .getMessage(response.getType(), RPCMessage.Type.RPC_FILE_WRITE_RESPONSE));
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      mHandler.removeListener(listener);
    }
  }
}
