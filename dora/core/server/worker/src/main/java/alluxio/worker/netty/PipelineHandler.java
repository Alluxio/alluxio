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

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.underfs.UfsManager;
import alluxio.worker.dora.DoraWorker;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Adds the data server's pipeline into the channel.
 */
@ThreadSafe
final class PipelineHandler extends ChannelInitializer<Channel> {
  private final FileTransferType mFileTransferType;
  private final UfsManager mUfsManager;
  private final DoraWorker mDoraWorker;

  /**
   *
   * @param ufsManager
   * @param doraWorker
   */
  public PipelineHandler(
      UfsManager ufsManager,
      DoraWorker doraWorker) {
    mUfsManager = ufsManager;
    mDoraWorker = doraWorker;

    mFileTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
  }

  @Override
  protected void initChannel(Channel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    final long timeoutMs = Configuration.getMs(PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS);

    // Decoders & Encoders
    pipeline.addLast("frameDecoder", RPCMessage.createFrameDecoder());
    pipeline.addLast("RPCMessageDecoder", new RPCMessageDecoder());
    pipeline.addLast("RPCMessageEncoder", new RPCMessageEncoder());

    // Idle Event Handlers
    pipeline.addLast("idleEventHandler", new IdleStateHandler(timeoutMs, 0, 0,
        TimeUnit.MILLISECONDS));
    pipeline.addLast("idleReadHandler", new IdleReadHandler());
    pipeline.addLast("heartbeatHandler", new HeartbeatHandler());

    // Block Handlers
    addBlockHandlerForDora(pipeline);

    // UFS Handlers
    pipeline.addLast("ufsFileWriteHandler", new UfsFileWriteHandler(
        NettyExecutors.UFS_WRITER_EXECUTOR, mUfsManager));
    // Unsupported Message Handler
    pipeline.addLast("unsupportedMessageHandler", new UnsupportedMessageHandler());
  }

  private void addBlockHandlerForDora(ChannelPipeline pipeline) {
    pipeline.addLast("fileReadHandler",
        new FileReadHandler(NettyExecutors.READER_EXECUTOR, mDoraWorker, mFileTransferType));
    //TODO(JiamingMai): WriteHandle also needs to be replaced, but it has not been implemented yet
    pipeline.addLast("fileWriteHandler",
        new FileWriteHandler(NettyExecutors.WRITER_EXECUTOR, mDoraWorker));
  }
}
