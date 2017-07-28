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
import alluxio.util.CommonUtils;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.channels.GatheringByteChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block write request. Check more information in
 * {@link DataServerWriteHandler}.
 */
@NotThreadSafe
public final class DataServerBlockWriteHandler extends DataServerWriteHandler {
  private static final long FILE_BUFFER_SIZE = Configuration.getBytes(
      PropertyKey.WORKER_FILE_BUFFER_SIZE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  private long mBytesReserved = 0;

  private class BlockWriteRequestInternal extends WriteRequestInternal {
    BlockWriter mBlockWriter;
    Counter mCounter;

    BlockWriteRequestInternal(Protocol.WriteRequest request) throws Exception {
      super(request.getId());
      Preconditions.checkState(request.getOffset() == 0);
      mWorker.createBlockRemote(mSessionId, mId, mStorageTierAssoc.getAlias(request.getTier()),
          FILE_BUFFER_SIZE);
      mBytesReserved = FILE_BUFFER_SIZE;
    }

    @Override
    public void close(Channel channel) throws IOException {
      if (mBlockWriter != null) {
        mBlockWriter.close();
      }
      try {
        mWorker.commitBlock(mSessionId, mId);
      } catch (Exception e) {
        throw CommonUtils.castToIOException(e);
      }
    }

    @Override
    void cancel() throws IOException {
      if (mBlockWriter != null) {
        mBlockWriter.close();
      }
      try {
        mWorker.abortBlock(mSessionId, mId);
      } catch (Exception e) {
        throw CommonUtils.castToIOException(e);
      }
    }

    @Override
    void cleanup() throws IOException {
      mWorker.cleanupSession(mSessionId);
    }
  }

  /**
   * Creates an instance of {@link DataServerBlockWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   * @param blockWorker the block worker
   */
  DataServerBlockWriteHandler(ExecutorService executorService, BlockWorker blockWorker) {
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

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   */
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      Protocol.WriteRequest request = (msg.getMessage()).asWriteRequest();
      mRequest = new BlockWriteRequestInternal(request);
    }
  }

  @Override
  protected void writeBuf(Channel channel, ByteBuf buf, long pos) throws Exception {
    if (mBytesReserved < pos) {
      long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - mBytesReserved);
      // Allocate enough space in the existing temporary block for the write.
      mWorker.requestSpace(mRequest.mSessionId, mRequest.mId, bytesToReserve);
      mBytesReserved += bytesToReserve;
    }
    BlockWriteRequestInternal request = (BlockWriteRequestInternal) mRequest;
    if (request.mBlockWriter == null) {
      request.mBlockWriter = mWorker.getTempBlockWriterRemote(request.mSessionId, request.mId);
      request.mCounter = MetricsSystem.workerCounter("BytesWrittenAlluxio");
    }
    GatheringByteChannel outputChannel = request.mBlockWriter.getChannel();
    int sz = buf.readableBytes();
    Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    Counter counter = ((BlockWriteRequestInternal) mRequest).mCounter;
    if (counter == null) {
      throw new IllegalStateException("metric counter is null");
    }
    counter.inc(bytesWritten);
  }
}
