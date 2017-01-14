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
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannelV2;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block read request. Check more information in {@link DataServerReadHandler}.
 */
@NotThreadSafe
public final class DataServerBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  /**
   * The block read request internal representation.
   */
  private final class BlockReadRequestInternal extends ReadRequestInternal {
    public BlockReader mBlockReader = null;

    /**
     * Creates an instance of {@link BlockReadRequestInternal}.
     *
     * @param request the block read request
     * @throws Exception if it fails to create the object
     */
    public BlockReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      mBlockReader = mWorker
          .readBlockRemote(request.getSessionId(), request.getId(), request.getLockId());
      mId = request.getId();
      mWorker.accessBlock(request.getSessionId(), mId);

      mStart = request.getOffset();
      mEnd = mStart + request.getLength();
      ((FileChannel) mBlockReader.getChannel()).position(mStart);
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
   * @param fileTransferType the file transfer type
   */
  public DataServerBlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
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
    Protocol.ReadRequest request = (Protocol.ReadRequest) ((RPCProtoMessage) object).getMessage();
    return request.getType() == Protocol.RequestType.ALLUXIO_BLOCK;
  }

  @Override
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new BlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws IOException {
    LocalFileBlockReader blockReader =
        (LocalFileBlockReader) ((BlockReadRequestInternal) mRequest).mBlockReader;
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
        return new DataFileChannelV2(new File(blockReader.getFilePath()), offset, len);
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
