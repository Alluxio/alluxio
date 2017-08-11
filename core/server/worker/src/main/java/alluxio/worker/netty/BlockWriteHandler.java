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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block write request. Check more information in
 * {@link AbstractWriteHandler}.
 */
@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class BlockWriteHandler extends AbstractWriteHandler<BlockWriteRequestContext> {
  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * Creates an instance of {@link BlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  BlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker) {
    super(executorService);
    mWorker = blockWorker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().asWriteRequest();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected PacketWriter createPacketWriter(BlockWriteRequestContext context, Channel channel) {
    return new BlockPacketWriter(context, channel, mWorker);
  }

  @Override
  protected BlockWriteRequestContext createRequestContext(Protocol.WriteRequest msg)
      throws Exception {
    BlockWriteRequestContext context = new BlockWriteRequestContext(msg, FILE_BUFFER_SIZE);
    WriteRequest request = context.getRequest();
    mWorker.createBlockRemote(request.getSessionId(), request.getId(),
        mStorageTierAssoc.getAlias(msg.getTier()), FILE_BUFFER_SIZE);
    return context;
  }

  /**
   * The packet writer that writes to a local block worker.
   */
  public class BlockPacketWriter extends PacketWriter {
    /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
    private final BlockWorker mWorker;

    /**
     * @param context context of this packet writer
     * @param channel netty channel
     * @param worker local block worker
     */
    public BlockPacketWriter(
        BlockWriteRequestContext context, Channel channel, BlockWorker worker) {
      super(context, channel);
      mWorker = worker;
    }

    @Override
    protected void completeRequest(BlockWriteRequestContext context, Channel channel)
        throws Exception {
      if (context == null) {
        return;
      }
      WriteRequest request = context.getRequest();
      if (context.getBlockWriter() != null) {
        context.getBlockWriter().close();
      }
      mWorker.commitBlock(request.getSessionId(), request.getId());
    }

    @Override
    protected void cancelRequest(BlockWriteRequestContext context) throws Exception {
      if (context == null) {
        return;
      }
      WriteRequest request = context.getRequest();
      if (context.getBlockWriter() != null) {
        context.getBlockWriter().close();
      }
      mWorker.abortBlock(request.getSessionId(), request.getId());
    }

    @Override
    protected void cleanupRequest(BlockWriteRequestContext context) throws Exception {
      if (context == null) {
        return;
      }
      WriteRequest request = context.getRequest();
      mWorker.cleanupSession(request.getSessionId());
    }

    @Override
    protected void writeBuf(BlockWriteRequestContext context, Channel channel, ByteBuf buf,
        long pos) throws Exception {
      Preconditions.checkState(context != null);
      WriteRequest request = context.getRequest();
      long bytesReserved = context.getBytesReserved();
      if (bytesReserved < pos) {
        long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - bytesReserved);
        // Allocate enough space in the existing temporary block for the write.
        mWorker.requestSpace(request.getSessionId(), request.getId(), bytesToReserve);
        context.setBytesReserved(bytesReserved + bytesToReserve);
      }
      if (context.getBlockWriter() == null) {
        context.setBlockWriter(
            mWorker.getTempBlockWriterRemote(request.getSessionId(), request.getId()));
        context.setCounter(MetricsSystem.workerCounter("BytesWrittenAlluxio"));
      }
      Preconditions.checkState(context.getBlockWriter() != null);
      GatheringByteChannel outputChannel = context.getBlockWriter().getChannel();
      int sz = buf.readableBytes();
      Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
    }
  }
}
