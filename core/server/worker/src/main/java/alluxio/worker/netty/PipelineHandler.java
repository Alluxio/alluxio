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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.worker.AlluxioWorkerService;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Adds the data server's pipeline into the channel.
 */
@ThreadSafe
final class PipelineHandler extends ChannelInitializer<SocketChannel> {
  private final AlluxioWorkerService mWorker;
  private final FileTransferType mFileTransferType;

  /**
   * @param worker the Alluxio worker
   */
  public PipelineHandler(AlluxioWorkerService worker) {
    mWorker = worker;
    mFileTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    // Decoders & Encoders
    pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
    pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
    pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());

    // Block Handlers
    pipeline.addLast("dataServerBlockReadHandler", new DataServerBlockReadHandler(
        NettyExecutors.BLOCK_READER_EXECUTOR, mWorker.getBlockWorker(), mFileTransferType));
    pipeline.addLast("dataServerBlockWriteHandler", new DataServerBlockWriteHandler(
        NettyExecutors.BLOCK_WRITER_EXECUTOR, mWorker.getBlockWorker()));

    // UFS Handlers
    pipeline.addLast("dataServerUfsBlockReadHandler", new DataServerUfsBlockReadHandler(
        NettyExecutors.UFS_BLOCK_READER_EXECUTOR, mWorker.getBlockWorker()));
    pipeline.addLast("dataServerUfsFileWriteHandler", new DataServerUfsFileWriteHandler(
        NettyExecutors.FILE_WRITER_EXECUTOR));

    // Unsupported Message Handler
    pipeline.addLast("dataServerUnsupportedMessageHandler", new
        DataServerUnsupportedMessageHandler());
  }
}
