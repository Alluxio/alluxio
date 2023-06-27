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

import alluxio.DefaultStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.status.NotFoundException;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

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
 * This handler handles block read request. Check more information in {@link AbstractReadHandler}.
 */
@alluxio.annotation.SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
    justification = "false positive with superclass generics, "
        + "see more description in https://sourceforge.net/p/findbugs/bugs/1242/")
@NotThreadSafe
public final class BlockReadHandler extends AbstractReadHandler<BlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService  the executor service to run {@link PacketReader}s
   * @param blockWorker      the block worker
   * @param channel          the channel this handler is attached to
   * @param fileTransferType the file transfer type
   */
  public BlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
                          Channel channel, FileTransferType fileTransferType) {
    super(executorService, channel, BlockReadRequest.class,
        new BlockPacketReaderFactory(blockWorker, fileTransferType));
  }

  @Override
  protected BlockReadRequest createReadRequest(Protocol.ReadRequest request) {
    return new BlockReadRequest(request);
  }

  /**
   * The packet reader to read from a local block worker.
   */
  @NotThreadSafe
  public static final class BlockPacketReader implements PacketReader<BlockReadRequest> {
    private final BlockReader mBlockReader;
    private final BlockReadRequest mReadRequest;
    private final FileTransferType mTransferType;

    BlockPacketReader(BlockReader blockReader, BlockReadRequest blockReadRequest,
        FileTransferType transferType) {
      mBlockReader = blockReader;
      mReadRequest = blockReadRequest;
      mTransferType = transferType;
    }

    @Override
    public DataBuffer getDataBuffer(Channel channel, long offset, int len) throws Exception {
      if (mTransferType == FileTransferType.TRANSFER
          && (mBlockReader instanceof LocalFileBlockReader)) {
        return new DataFileChannel(new File(((LocalFileBlockReader) mBlockReader).getFilePath()),
            offset, len);
      } else {
        ByteBuf buf = channel.alloc().buffer(len, len);
        try {
          while (buf.writableBytes() > 0 && mBlockReader.transferTo(buf) != -1) {
          }
          return new NettyDataBuffer(buf);
        } catch (Throwable e) {
          buf.release();
          throw e;
        }
      }
    }

    @Override
    public void close() throws IOException {
      try {
        mBlockReader.close();
      } catch (Exception e) {
        LOG.warn("Failed to close block reader for block {} with error {}.",
            mReadRequest.getId(), e.getMessage());
      }
    }
  }

  /**
   * BlockPacketReaderFactory.
   */
  public static class BlockPacketReaderFactory
      implements PacketReader.Factory<BlockReadRequest, BlockPacketReader> {
    /**
     * An object storing the mapping of tier aliases to ordinals.
     */
    private final StorageTierAssoc mStorageTierAssoc = new DefaultStorageTierAssoc(
        PropertyKey.WORKER_TIERED_STORE_LEVELS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS);
    private final BlockWorker mWorker;
    private final FileTransferType mTransferType;

    /**
     * Constructor.
     *
     * @param blockWorker block worker
     * @param transferType transfer type
     */
    public BlockPacketReaderFactory(BlockWorker blockWorker, FileTransferType transferType) {
      mWorker = blockWorker;
      mTransferType = transferType;
    }

    @Override
    public BlockPacketReader create(BlockReadRequest request) throws IOException {
      if (request.isPromote()) {
        try {
          mWorker.moveBlock(request.getSessionId(), request.getId(), mStorageTierAssoc.getAlias(0));
        } catch (BlockDoesNotExistException e) {
          LOG.debug("Block {} to promote does not exist in Alluxio: {}", request.getId(),
              e.getMessage());
        } catch (Exception e) {
          LOG.warn("Failed to promote block {}: {}", request.getId(), e.getMessage());
        }
      }
      BlockReader reader = mWorker.createBlockReader(request.getSessionId(), request.getId(),
          request.getStart(), false, request.getOpenUfsBlockOptions());
      // TODO(bowen): add metric
      /*
      String metricName = "BytesReadAlluxio";
      context.setCounter(MetricsSystem.counter(metricName));
      */
      try {
        mWorker.accessBlock(request.getSessionId(), request.getId());
      } catch (BlockDoesNotExistException e) {
        throw new NotFoundException(e);
      }
      if (reader.getChannel() instanceof FileChannel) {
        ((FileChannel) reader.getChannel()).position(request.getStart());
      }
      return new BlockPacketReader(reader, request, mTransferType);

      /*
      ProtoMessage heartbeat = new ProtoMessage(Protocol.ReadResponse.newBuilder()
          .setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT).build());
      // Sends an empty buffer to the client to make sure that the client does not timeout when
      // the server is waiting for the UFS block access.
      channel.writeAndFlush(new RPCProtoMessage(heartbeat));
      */
    }
  }
}
