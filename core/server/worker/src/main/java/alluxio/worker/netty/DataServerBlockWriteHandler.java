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
    final BlockWriter mBlockWriter;

    BlockWriteRequestInternal(Protocol.WriteRequest request) throws Exception {
      super(request.getId(), request.getSessionId());
      Preconditions.checkState(request.getOffset() == 0);
      mWorker.createBlockRemote(request.getSessionId(), request.getId(),
          mStorageTierAssoc.getAlias(request.getTier()), FILE_BUFFER_SIZE);
      mBytesReserved = FILE_BUFFER_SIZE;
      mBlockWriter = mWorker.getTempBlockWriterRemote(request.getSessionId(), request.getId());
    }

    @Override
    public void close() throws IOException {
      mBlockWriter.close();
      // TODO(peis): We can call mWorker.commitBlock() here.
    }

    @Override
    void cancel() throws IOException {
      mBlockWriter.close();
      // TODO(peis): We can call mWorker.abortBlock() here.
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
    Protocol.WriteRequest request = ((RPCProtoMessage) object).getMessage().getMessage();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    super.initializeRequest(msg);
    if (mRequest == null) {
      Protocol.WriteRequest request = (msg.getMessage()).getMessage();
      mRequest = new BlockWriteRequestInternal(request);
    }
  }

  @Override
  protected void writeBuf(ByteBuf buf, long pos) throws Exception {
    if (mBytesReserved < pos) {
      long bytesToReserve = Math.max(FILE_BUFFER_SIZE, pos - mBytesReserved);
      // Allocate enough space in the existing temporary block for the write.
      mWorker.requestSpace(mRequest.mSessionId, mRequest.mId, bytesToReserve);
      mBytesReserved += bytesToReserve;
    }
    BlockWriter blockWriter = ((BlockWriteRequestInternal) mRequest).mBlockWriter;
    GatheringByteChannel outputChannel = blockWriter.getChannel();
    int sz = buf.readableBytes();
    Preconditions.checkState(buf.readBytes(outputChannel, sz) == sz);
  }

  @Override
  protected void incrementMetrics(long bytesWritten) {
    Metrics.BYTES_WRITTEN_REMOTE.inc(bytesWritten);
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_WRITTEN_REMOTE =
        MetricsSystem.workerCounter("BytesWrittenRemote");

    private Metrics() {
    } // prevent instantiation
  }
}
