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
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;
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
public final class BlockWriteHandler
    extends AbstractWriteHandler<BlockWriteHandler.BlockWriteRequest> {
  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * The block write request internal representation. When this request is complete, we need to
   * commit the block.
   */
  @NotThreadSafe
  static final class BlockWriteRequest extends WriteRequest {
    private final Context mContext = new Context();

    BlockWriteRequest(Protocol.WriteRequest request, long bytesReserved) throws Exception {
      super(request.getId());
      Preconditions.checkState(request.getOffset() == 0);
      mContext.setBytesReserved(bytesReserved);
    }

    /**
     * @return the context of this request
     */
    public Context getContext() {
      return mContext;
    }

    /**
     * The context of this request, including some runtime state to handle this request.
     */
    @NotThreadSafe
    final class Context {
      private BlockWriter mBlockWriter;
      private Counter mCounter;
      private long mBytesReserved;

      Context() {}

      /**
       * @return the block writer
       */
      @Nullable
      public BlockWriter getBlockWriter() {
        return mBlockWriter;
      }

      /**
       * @return the bytes reserved
       */
      public long getBytesReserved() {
        return mBytesReserved;
      }

      /**
       * @return the counter
       */
      @Nullable
      public Counter getCounter() {
        return mCounter;
      }

      /**
       * Sets the block writer.
       *
       * @param blockWriter block writer to set
       */
      public void setBlockWriter(BlockWriter blockWriter) {
        mBlockWriter = blockWriter;
      }

      /**
       * Sets the bytes reserved.
       *
       * @param bytesReserved the bytes reserved to set
       */
      public void setBytesReserved(long bytesReserved) {
        mBytesReserved = bytesReserved;
      }

      /**
       * Sets the counter.
       *
       * @param counter counter to set
       */
      public void setCounter(Counter counter) {
        mCounter = counter;
      }
    }
  }

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
  protected BlockWriteRequest createRequest(RPCProtoMessage msg) throws Exception {
    Protocol.WriteRequest requestProto = (msg.getMessage()).asWriteRequest();
    BlockWriteRequest request = new BlockWriteRequest(requestProto, FILE_BUFFER_SIZE);
    mWorker.createBlockRemote(request.getSessionId(), request.getId(),
        mStorageTierAssoc.getAlias(requestProto.getTier()), FILE_BUFFER_SIZE);
    return request;
  }

  @Override
  protected void completeRequest(Channel channel) throws Exception {
    BlockWriteRequest request = getRequest();
    Preconditions.checkState(request != null);

    if (request.getContext().getBlockWriter() != null) {
      request.getContext().getBlockWriter().close();
    }
    mWorker.commitBlock(request.getSessionId(), request.getId());
  }

  @Override
  protected void cancelRequest() throws Exception {
    BlockWriteRequest request = getRequest();
    Preconditions.checkState(request != null);

    if (request.getContext().getBlockWriter() != null) {
      request.getContext().getBlockWriter().close();
    }
    mWorker.abortBlock(request.getSessionId(), request.getId());
  }

  @Override
  protected void cleanupRequest() throws Exception {
    BlockWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    mWorker.cleanupSession(request.getSessionId());
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    BlockWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    long bytesReserved = request.getContext().getBytesReserved();
    if (bytesReserved < pos) {
      long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - bytesReserved);
      // Allocate enough space in the existing temporary block for the write.
      mWorker.requestSpace(request.getSessionId(), request.getId(), bytesToReserve);
      request.getContext().setBytesReserved(bytesReserved + bytesToReserve);
    }
    if (request.getContext().getBlockWriter() == null) {
      request.getContext().setBlockWriter(mWorker.getTempBlockWriterRemote(
          request.getSessionId(), request.getId()));
      request.getContext().setCounter(MetricsSystem.workerCounter("BytesWrittenAlluxio"));
    }
    Preconditions.checkState(request.getContext().getBlockWriter() != null);
    GatheringByteChannel outputChannel = request.getContext().getBlockWriter().getChannel();
    int sz = buf.readableBytes();
    Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    BlockWriteRequest request = getRequest();
    Preconditions.checkState(request != null);
    Counter counter = request.getContext().getCounter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesWritten);
  }
}
