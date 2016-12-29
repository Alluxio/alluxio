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
import alluxio.client.UnderFileSystemFileWriter;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.ExceptionMessage;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCFileWriteResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataByteArrayChannel;

import com.codahale.metrics.Counter;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Writer for an under file system file through a worker data server via Netty. This class does not
 * hold any state specific to the file and can be used to write to different files, but not
 * concurrently. This class does not keep lingering resources and does not need to be closed.
 */
@NotThreadSafe
public final class NettyUnderFileSystemFileWriter implements UnderFileSystemFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final FileSystemContext mContext;

  /**
   * Constructor for a Netty based writer to an under file system file on a worker.
   * @param context the file system context
   */
  public NettyUnderFileSystemFileWriter(FileSystemContext context) {
    mContext = context;
  }

  @Override
  public void write(InetSocketAddress address, long ufsFileId, long fileOffset, byte[] source,
      int offset, int length) throws IOException {
    Channel channel = null;
    ClientHandler clientHandler = null;
    Metrics.NETTY_UFS_WRITE_OPS.inc();
    try {
      channel = mContext.acquireNettyChannel(address);
      if (!(channel.pipeline().last() instanceof ClientHandler)) {
        channel.pipeline().addLast(new ClientHandler());
      }
      clientHandler = (ClientHandler) channel.pipeline().last();
      SingleResponseListener listener = new SingleResponseListener();
      clientHandler.addListener(listener);

      ChannelFuture channelFuture = channel.writeAndFlush(
          new RPCFileWriteRequest(ufsFileId, fileOffset, length,
              new DataByteArrayChannel(source, offset, length))).sync();

      if (channelFuture.isDone() && !channelFuture.isSuccess()) {
        LOG.error("Failed to read ufs file from %s for ufsFilId %d with error %s.",
            address.toString(), ufsFileId, channelFuture.cause());
        throw new IOException(channelFuture.cause());
      }

      RPCResponse response = listener.get(NettyClient.TIMEOUT_MS, TimeUnit.MILLISECONDS);

      switch (response.getType()) {
        case RPC_FILE_WRITE_RESPONSE:
          RPCFileWriteResponse resp = (RPCFileWriteResponse) response;
          RPCResponse.Status status = resp.getStatus();
          LOG.debug("status: {} from remote machine {} received", status, address);

          if (status != RPCResponse.Status.SUCCESS) {
            throw new IOException(ExceptionMessage.UNDER_FILE_WRITE_ERROR.getMessage(ufsFileId,
                address, status.getMessage()));
          }
          break;
        case RPC_ERROR_RESPONSE:
          RPCErrorResponse error = (RPCErrorResponse) response;
          throw new IOException(error.getStatus().getMessage());
        default:
          throw new IOException(ExceptionMessage.UNEXPECTED_RPC_RESPONSE
              .getMessage(response.getType(), RPCMessage.Type.RPC_FILE_WRITE_RESPONSE));
      }
    } catch (Exception e) {
      Metrics.NETTY_UFS_WRITE_FAILURES.inc();
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

  @Override
  public void close() {}

  /**
   * Class that contains metrics about {@link NettyUnderFileSystemFileWriter}.
   */
  @ThreadSafe
  private static final class Metrics {
    private static final Counter NETTY_UFS_WRITE_OPS =
        MetricsSystem.clientCounter("NettyUfsWriteOps");
    private static final Counter NETTY_UFS_WRITE_FAILURES =
        MetricsSystem.clientCounter("NettyUfsWriteFailures");

    private Metrics() {} // prevent instantiation
  }
}
