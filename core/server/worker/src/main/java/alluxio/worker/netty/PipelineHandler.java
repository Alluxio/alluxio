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
import alluxio.worker.WorkerProcess;
import alluxio.worker.block.AsyncCacheRequestManager;
import alluxio.worker.block.BlockWorker;

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
  private final WorkerProcess mWorkerProcess;
  private final FileTransferType mFileTransferType;
  private final AsyncCacheRequestManager mRequestManager;

  /**
   * @param workerProcess the Alluxio worker process
   */
  public PipelineHandler(WorkerProcess workerProcess) {
    mWorkerProcess = workerProcess;
    mFileTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
    mRequestManager = new AsyncCacheRequestManager(
        NettyExecutors.ASYNC_CACHE_MANAGER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class));
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
    pipeline.addLast("blockReadHandler",
        new BlockReadHandler(NettyExecutors.BLOCK_READER_EXECUTOR,
            mWorkerProcess.getWorker(BlockWorker.class), mFileTransferType));
    pipeline.addLast("blockWriteHandler", new BlockWriteHandler(
        NettyExecutors.BLOCK_WRITER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class),
        mWorkerProcess.getUfsManager()));
    pipeline.addLast("shortCircuitBlockReadHandler",
        new ShortCircuitBlockReadHandler(NettyExecutors.RPC_EXECUTOR,
            mWorkerProcess.getWorker(BlockWorker.class)));
    pipeline.addLast("shortCircuitBlockWriteHandler",
        new ShortCircuitBlockWriteHandler(NettyExecutors.RPC_EXECUTOR,
            mWorkerProcess.getWorker(BlockWorker.class)));
    pipeline.addLast("asyncCacheHandler", new AsyncCacheHandler(mRequestManager));

    // UFS Handlers
    pipeline.addLast("ufsFileWriteHandler", new UfsFileWriteHandler(
        NettyExecutors.FILE_WRITER_EXECUTOR, mWorkerProcess.getUfsManager()));

    // Endpoint for UFS-fallback block write
    pipeline.addLast("ufsFallbackBlockWriteHandler", new UfsFallbackBlockWriteHandler(
        NettyExecutors.FILE_WRITER_EXECUTOR, mWorkerProcess.getWorker(BlockWorker.class),
        mWorkerProcess.getUfsManager()));

    // RPC handler.
    pipeline.addLast("dataServerRPCHandler", new RPCHandler(NettyExecutors.RPC_EXECUTOR,
        mWorkerProcess.getWorker(BlockWorker.class)));

    // Unsupported Message Handler
    pipeline.addLast("unsupportedMessageHandler", new UnsupportedMessageHandler());
  }
}
