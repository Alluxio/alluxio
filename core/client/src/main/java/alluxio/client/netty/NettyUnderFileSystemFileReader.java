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

import alluxio.client.UnderFileSystemFileReader;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileReadResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Reader for an under file system file through a worker's data server. This class does not hold
 * any state specific to the file being read and can be used to read multiple files, but not
 * concurrently. Close should be called on the reader if the caller no longer intends to use the
 * reader in order to clean up any lingering data from the last read.
 */
@NotThreadSafe
public final class NettyUnderFileSystemFileReader implements UnderFileSystemFileReader {
  private static final Logger LOG = LoggerFactory.getLogger(NettyUnderFileSystemFileReader.class);

  private final FileSystemContext mContext;

  /** A reference to read response so we can explicitly release the resource after reading. */
  private RPCFileReadResponse mReadResponse;

  /**
   * Creates a new reader for a file in an under file system through a worker's data server.
   * @param context the file system context
   */
  public NettyUnderFileSystemFileReader(FileSystemContext context) {
    mContext = Preconditions.checkNotNull(context);
  }

  @Override
  public ByteBuffer read(InetSocketAddress address, long ufsFileId, long offset, long length)
      throws IOException {
    // For a zero length read, directly return without trying the Netty call.
    if (length == 0) {
      return ByteBuffer.allocate(0);
    }

    Metrics.NETTY_UFS_READ_OPS.inc();
    Channel channel = null;
    ClientHandler clientHandler = null;
    try {
      channel = mContext.acquireNettyChannel(address);
      if (!(channel.pipeline().last() instanceof ClientHandler)) {
        channel.pipeline().addLast(new ClientHandler());
      }
      clientHandler = (ClientHandler) channel.pipeline().last();
      SingleResponseListener listener = new SingleResponseListener();
      clientHandler.addListener(listener);

      ChannelFuture channelFuture =
          channel.writeAndFlush(new RPCFileReadRequest(ufsFileId, offset, length)).sync();

      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to read ufs file from %s for ufsFilId %d with error %s.",
            address.toString(), ufsFileId, channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

      switch (response.getType()) {
        case RPC_FILE_READ_RESPONSE:
          RPCFileReadResponse resp = (RPCFileReadResponse) response;
          LOG.debug("Data for ufs file id {} from machine {} received", ufsFileId, address);
          RPCResponse.Status status = resp.getStatus();
          if (status == RPCResponse.Status.SUCCESS) {
            // always clear the previous response before reading another one
            cleanup();
            // End of file reached
            if (resp.isEOF()) {
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
      Metrics.NETTY_UFS_READ_FAILURES.inc();
      try {
        if (channel != null) {
          channel.close().sync();
        }
      } catch (InterruptedException ee) {
        Throwables.propagate(ee);
      }
      throw new IOException(e);
    } finally {
      if (clientHandler != null) {
        clientHandler.removeListeners();
      }
      if (channel != null) {
        mContext.releaseNettyChannel(address, channel);
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

  /**
   * Class that contains metrics about {@link NettyUnderFileSystemFileReader}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_UFS_READ_OPS =
        MetricsSystem.clientCounter("NettyUfsReadOps");
    private static final Counter NETTY_UFS_READ_FAILURES =
        MetricsSystem.clientCounter("NettyUfsReadFailures");

    private Metrics() {} // prevent instantiation
  }
}
