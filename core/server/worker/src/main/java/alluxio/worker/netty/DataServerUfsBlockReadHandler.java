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

import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block read request. Check more information in
 * {@link DataServerReadHandler}.
 */
@NotThreadSafe
final class DataServerUfsBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerUfsBlockReadHandler.class);

  /** The Block Worker. */
  private final BlockWorker mWorker;

  /**
   * The block read request internal representation.
   */
  private final class UfsBlockReadRequestInternal extends ReadRequestInternal {
    final BlockReader mBlockReader;

    /**
     * Creates an instance of {@link UfsBlockReadRequestInternal}.
     *
     * @param request the block read request
     */
    UfsBlockReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      super(request.getId(), request.getOffset(), request.getOffset() + request.getLength());
      mBlockReader =
          mWorker.readUfsBlock(request.getSessionId(), mId, mStart, request.getNoCache());
      // Note that we do not need to seek to offset since the block worker is created at the offset.
    }

    @Override
    public void close() {
      if (mBlockReader != null)  {
        try {
          mBlockReader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {}.", mId);
        }
      }
    }
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   * @param blockWorker the block worker
   */
  public DataServerUfsBlockReadHandler(ExecutorService executorService, BlockWorker blockWorker) {
    super(executorService);
    mWorker = blockWorker;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.ReadRequest request = ((RPCProtoMessage) object).getMessage().getMessage();
    return request.getType() == Protocol.RequestType.UFS_BLOCK;
  }

  @Override
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new UfsBlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws IOException {
    BlockReader blockReader = ((UfsBlockReadRequestInternal) mRequest).mBlockReader;
    // This buf is released by netty.
    ByteBuf buf = channel.alloc().buffer(len, len);
    try {
      while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
      }
      return new DataNettyBufferV2(buf);
    } catch (Throwable e) {
      buf.release();
      throw e;
    }
  }

  @Override
  protected void incrementMetrics(long bytesRead) {
    Metrics.BYTES_READ_UFS.inc(bytesRead);
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_UFS = MetricsSystem.workerCounter("BytesReadUFS");

    private Metrics() {
    } // prevent instantiation
  }
}
