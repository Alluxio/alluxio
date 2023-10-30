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

import alluxio.AlluxioURI;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.CompositeDataBuffer;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.dora.PagedFileReader;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Handles file read request.
 */
public class FileReadHandler extends AbstractReadHandler<BlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(FileReadHandler.class);

  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);
  private final DoraWorker mWorker;

  /**
   * Creates an instance of {@link FileReadHandler}.
   *
   * @param executorService the executor service to run data readers
   * @param channel the channel to which this handler is attached
   * @param worker block worker
   * @param fileTransferType the file transfer type
   */
  public FileReadHandler(ExecutorService executorService, Channel channel,
                         DoraWorker worker, FileTransferType fileTransferType) {
    super(executorService, channel, BlockReadRequest.class,
        new FilePacketReaderFactory(worker, fileTransferType));
    mWorker = worker;
  }

  @Override
  protected BlockReadRequest createReadRequest(Protocol.ReadRequest request) {
    return new BlockReadRequest(request);
  }

  /**
   * Gets worker.
   * @return dora worker
   */
  public DoraWorker getWorker() {
    return mWorker;
  }

  /**
   * Factory for creating {@link FilePacketReader}s.
   */
  public static final class FilePacketReaderFactory
      implements PacketReader.Factory<BlockReadRequest, FilePacketReader> {
    private final DoraWorker mWorker;
    private final FileTransferType mTransferType;

    /**
     * Constructor.
     *
     * @param doraWorker dora worker
     * @param transferType transfer type
     */
    public FilePacketReaderFactory(DoraWorker doraWorker, FileTransferType transferType) {
      mWorker = doraWorker;
      mTransferType = transferType;
    }

    @Override
    public FilePacketReader create(BlockReadRequest readRequest) throws IOException {
      try {
        final String fileId =
            new AlluxioURI(readRequest.getOpenUfsBlockOptions().getUfsPath()).hash();
        BlockReader reader =
            mWorker.createFileReader(fileId, readRequest.getStart(),
                /* positionShort */ false, readRequest.getOpenUfsBlockOptions());
        if (reader.getChannel() instanceof FileChannel) {
          ((FileChannel) reader.getChannel()).position(readRequest.getStart());
        }
        return new FilePacketReader(reader, readRequest, mTransferType);
      } catch (AccessControlException e) {
        throw new PermissionDeniedException(e);
      }

      // TODO(bowen): add metrics
      /*
      String metricName = "BytesReadAlluxio";
      context.setCounter(MetricsSystem.counter(metricName));
      */

      //TODO(JiamingMai): Not sure if this is necessary.
      /*
      ProtoMessage heartbeat = new ProtoMessage(Protocol.ReadResponse.newBuilder()
          .setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT).build());
      // Sends an empty buffer to the client to make sure that the client does not timeout when
      // the server is waiting for the UFS block access.
      channel.writeAndFlush(new RPCProtoMessage(heartbeat));
      */
    }
  }

  /**
   * The packet reader to read from a local block worker.
   */
  @NotThreadSafe
  public static final class FilePacketReader implements PacketReader<BlockReadRequest> {
    private final BlockReader mReader;
    private final BlockReadRequest mReadRequest;
    private final FileTransferType mTransferType;

    FilePacketReader(BlockReader reader, BlockReadRequest request, FileTransferType transferType) {
      mReader = reader;
      mReadRequest = request;
      mTransferType = transferType;
    }

    @Override
    public DataBuffer createDataBuffer(Channel channel, long offset, int len)
        throws Exception {
      if (mTransferType == FileTransferType.TRANSFER) {
        if (mReader instanceof PagedFileReader) {
          PagedFileReader pagedFileReader = (PagedFileReader) mReader;
          CompositeDataBuffer compositeDataBuffer =
              pagedFileReader.getMultipleDataFileChannel(channel, len);
          return compositeDataBuffer;
        } else {
          throw new UnsupportedOperationException(mReader.getClass().getCanonicalName()
              + "is no longer supported in Alluxio 3.x");
        }
      }
      return createDataBufferByCopying(channel, len);
    }

    private DataBuffer createDataBufferByCopying(Channel channel, int len)
        throws IOException {
      ByteBuf buf = channel.alloc().buffer(len, len);
      try {
        while (buf.writableBytes() > 0 && mReader.transferTo(buf) != -1) {
        }
        return new NettyDataBuffer(buf);
      } catch (Throwable e) {
        buf.release();
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        mReader.close();
      } catch (Exception e) {
        LOG.warn("Failed to close reader for file {} with error {}.",
            mReadRequest.getOpenUfsBlockOptions().getUfsPath(), e.getMessage());
      }
    }
  }
}
