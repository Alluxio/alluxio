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
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block read request. Check more information in {@link DataServerReadHandler}.
 */
@NotThreadSafe
final class DataServerBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerBlockReadHandler.class);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  /**
   * The block read request internal representation.
   */
  private final class BlockReadRequestInternal extends ReadRequestInternal {
    final BlockReader mBlockReader;

    /**
     * Creates an instance of {@link BlockReadRequestInternal}.
     *
     * @param request the block read request
     * @throws Exception if it fails to create the object
     */
    BlockReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      super(request.getId(), request.getOffset(), request.getOffset() + request.getLength());
      mBlockReader = mWorker
          .readBlockRemote(request.getSessionId(), request.getId(), request.getLockId());
      mWorker.accessBlock(request.getSessionId(), mId);

      ((FileChannel) mBlockReader.getChannel()).position(mStart);
    }

    @Override
    public void close() throws IOException {
      if (mBlockReader != null) {
        mBlockReader.close();
      }
    }
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   * @param blockWorker the block worker
   * @param fileTransferType the file transfer type
   */
  DataServerBlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
      FileTransferType fileTransferType) {
    super(executorService);
    mWorker = blockWorker;
    mTransferType = fileTransferType;
  }

  @Override
  protected boolean acceptMessage(Object object) {
    if (!super.acceptMessage(object)) {
      return false;
    }
    Protocol.ReadRequest request = ((RPCProtoMessage) object).getMessage().getMessage();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new BlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws IOException {
    BlockReader blockReader = ((BlockReadRequestInternal) mRequest).mBlockReader;
    Preconditions.checkArgument(blockReader.getChannel() instanceof FileChannel,
        "Only FileChannel is supported!");
    switch (mTransferType) {
      case MAPPED:
        ByteBuf buf = channel.alloc().buffer(len, len);
        try {
          FileChannel fileChannel = (FileChannel) blockReader.getChannel();
          Preconditions.checkState(fileChannel.position() == offset);
          while (buf.writableBytes() > 0
              && buf.writeBytes(fileChannel, buf.writableBytes()) != -1) {
          }
          return new DataNettyBufferV2(buf);
        } catch (Throwable e) {
          buf.release();
          throw e;
        }
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        return new DataFileChannel((FileChannel) blockReader.getChannel(), offset, len);
    }
  }

  @Override
  protected void incrementMetrics(long bytesRead) {
    Metrics.BYTES_READ_REMOTE.inc(bytesRead);
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.workerCounter("BytesReadRemote");

    private Metrics() {
    } // prevent instantiation
  }
}
