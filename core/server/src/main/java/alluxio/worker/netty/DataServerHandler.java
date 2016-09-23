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

package alluxio.worker.netty;

import alluxio.Constants;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCFileReadRequest;
import alluxio.network.protocol.RPCFileWriteRequest;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCRequest;
import alluxio.network.protocol.RPCResponse;
import alluxio.worker.AlluxioWorkerService;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class processes {@link RPCRequest} messages and delegates them to the appropriate
 * handlers to return {@link RPCResponse} messages.
 */
@ChannelHandler.Sharable
@NotThreadSafe
final class DataServerHandler extends SimpleChannelInboundHandler<RPCMessage> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Handler for any block store requests. */
  private final BlockDataServerHandler mBlockHandler;
  /** Handler for any file system requests. */
  private final UnderFileSystemDataServerHandler mUnderFileSystemHandler;

  /**
   * Creates a new instance of {@link DataServerHandler}.
   *
   * @param worker the Alluxio worker handle
   */
  public DataServerHandler(final AlluxioWorkerService worker) {
    Preconditions.checkNotNull(worker);
    mBlockHandler = new BlockDataServerHandler(worker.getBlockWorker());
    mUnderFileSystemHandler = new UnderFileSystemDataServerHandler(worker.getFileSystemWorker());
  }

  @Override
  public void channelRead0(final ChannelHandlerContext ctx, final RPCMessage msg)
      throws IOException {
    switch (msg.getType()) {
      case RPC_BLOCK_READ_REQUEST:
        assert msg instanceof RPCBlockReadRequest;
        mBlockHandler.handleBlockReadRequest(ctx, (RPCBlockReadRequest) msg);
        break;
      case RPC_BLOCK_WRITE_REQUEST:
        assert msg instanceof RPCBlockWriteRequest;
        mBlockHandler.handleBlockWriteRequest(ctx, (RPCBlockWriteRequest) msg);
        break;
      case RPC_FILE_READ_REQUEST:
        assert msg instanceof RPCFileReadRequest;
        mUnderFileSystemHandler.handleFileReadRequest(ctx, (RPCFileReadRequest) msg);
        break;
      case RPC_FILE_WRITE_REQUEST:
        assert msg instanceof RPCFileWriteRequest;
        mUnderFileSystemHandler.handleFileWriteRequest(ctx, (RPCFileWriteRequest) msg);
        break;
      case RPC_ERROR_RESPONSE:
        // TODO(peis): Fix this, we should not assert here.
        assert msg instanceof RPCErrorResponse;
        LOG.error("Received an error response from the client: " + msg.toString());
        break;
      default:
        RPCErrorResponse resp = new RPCErrorResponse(RPCResponse.Status.UNKNOWN_MESSAGE_ERROR);
        ctx.writeAndFlush(resp);
        // TODO(peis): Fix this. We should not throw an exception here.
        throw new IllegalArgumentException(
            "No handler implementation for rpc msg type: " + msg.getType());
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    // TODO(peis): This doesn't have to be decode error, it can also be any network errors such as
    // connection reset. Fix this ALLUXIO-2235.
    RPCErrorResponse resp = new RPCErrorResponse(RPCResponse.Status.DECODE_ERROR);
    ChannelFuture channelFuture = ctx.writeAndFlush(resp);
    // Close the channel because it is likely a network error.
    channelFuture.addListener(ChannelFutureListener.CLOSE);
  }
}
