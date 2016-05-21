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

package alluxio.client.netty;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileReadResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Reader for an under file system file through a worker's data server. This class does not hold
 * any state specific to the file being read and can be used to read multiple files, but not
 * concurrently. Close should be called on the reader if the caller no longer intends to use the
 * reader in order to clean up any lingering data from the last read.
 */
@NotThreadSafe
public final class NettyUnderFileSystemFileReader implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Netty bootstrap for the connection. */
  private final Bootstrap mClientBootstrap;
  /** Handler for Netty messages. */
  private final ClientHandler mHandler;
  /** A reference to read response so we can explicitly release the resource after reading. */
  private RPCFileReadResponse mReadResponse = null;

  /**
   * Creates a new reader for a file in an under file system through a worker's data server.
   */
  public NettyUnderFileSystemFileReader() {
    mHandler = new ClientHandler();
    mClientBootstrap = NettyClient.createClientBootstrap(mHandler);
  }

  /**
   * Reads data from the specified worker for a file in the under file system.
   *
   * @param address the worker address to read from
   * @param ufsFileId the worker specific file id referencing the file to read
   * @param offset the offset in the file to read from
   * @param length the length to read
   * @return a byte buffer with the requested data, null if EOF is reached
   * @throws IOException if an error occurs communicating with the worker
   */
  public ByteBuffer read(InetSocketAddress address, long ufsFileId, long offset, long length)
      throws IOException {
    SingleResponseListener listener = null;
    try {
      ChannelFuture f = mClientBootstrap.connect(address).sync();

      LOG.debug("Connected to remote machine {}", address);
      Channel channel = f.channel();
      listener = new SingleResponseListener();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCFileReadRequest(ufsFileId, offset, length));

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);
      channel.close().sync();

      switch (response.getType()) {
        case RPC_FILE_READ_RESPONSE:
          RPCFileReadResponse resp = (RPCFileReadResponse) response;
          LOG.debug("Data for ufs file id {} from machine {} received", ufsFileId, address);
          RPCResponse.Status status = resp.getStatus();
          if (status == RPCResponse.Status.SUCCESS) {
            // always clear the previous response before reading another one
            cleanup();
            // End of file reached
            if (resp.getLength() == -1) {
              return null;
            }
            mReadResponse = resp;
            return resp.getPayloadDataBuffer().getReadOnlyByteBuffer();
          }
          throw new IOException(status.getMessage() + " response: " + resp);
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE.getMessage(
              response.getType(), RPCMessage.Type.RPC_FILE_READ_RESPONSE));
      }
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      if (listener != null) {
        mHandler.removeListener(listener);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Releases the underlying buffer of previous/current read response.
   */
  @Override
  public void close() {
    cleanup();
  }

  /**
   * Releases the underlying buffer of previous/current read response.
   */
  private void cleanup() {
    if (mReadResponse != null) {
      mReadResponse.getPayloadDataBuffer().release();
      mReadResponse = null;
    }
  }
}
