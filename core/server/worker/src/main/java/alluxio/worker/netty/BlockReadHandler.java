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
import alluxio.DefaultStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.block.io.StoreBlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
public final class BlockReadHandler extends AbstractReadHandler<BlockReadRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /**
   * The Block Worker.
   */
  private final BlockWorker mWorker;
  /**
   * The transfer type used by the data server.
   */
  private final FileTransferType mTransferType;

  /**
   * The packet reader to read from a local block worker.
   */
  @NotThreadSafe
  public final class BlockPacketReader extends PacketReader {
    /**
     * The Block Worker.
     */
    private final BlockWorker mWorker;
    /**
     * An object storing the mapping of tier aliases to ordinals.
     */
    private final StorageTierAssoc mStorageTierAssoc = new DefaultStorageTierAssoc(
        PropertyKey.WORKER_TIERED_STORE_LEVELS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS);

    BlockPacketReader(BlockReadRequestContext context, Channel channel, BlockWorker blockWorker) {
      super(context, channel);
      mWorker = blockWorker;
    }

    @Override
    protected void completeRequest(BlockReadRequestContext context) throws Exception {
      BlockReader reader = context.getBlockReader();
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {} with error {}.",
              context.getRequest().getId(), e.getMessage());
        }
      }
    }

    @Override
    protected DataBuffer getDataBuffer(BlockReadRequestContext context, Channel channel,
                                       long offset, int len) throws Exception {
      openBlock(context, channel);
      BlockReader blockReader = context.getBlockReader();
      Preconditions.checkState(blockReader != null);
      DataBuffer dataBuffer = null;
      if (mTransferType == FileTransferType.TRANSFER) {
        dataBuffer = createDataFileChannel(blockReader, offset, len);
      }
      if (dataBuffer != null) {
        return dataBuffer;
      }
      // create a non-zero-copy DataBuffer
      ByteBuf buf = channel.alloc().buffer(len, len);
      try {
        while (buf.writableBytes() > 0 && blockReader.transferTo(buf) != -1) {
        }
        return new NettyDataBuffer(buf);
      } catch (Throwable e) {
        buf.release();
        throw e;
      }
    }

    private DataBuffer createDataFileChannel(BlockReader blockReader, long offset, long len) {
      if (blockReader instanceof LocalFileBlockReader) {
        return new DataFileChannel(new File(((LocalFileBlockReader) blockReader).getFilePath()),
            offset, len);
      }
      if (blockReader instanceof DelegatingBlockReader) {
        BlockReader delegatingBlockReader = ((DelegatingBlockReader) blockReader).getDelegate();
        if (delegatingBlockReader instanceof StoreBlockReader) {
          String path = ((StoreBlockReader) delegatingBlockReader).getFilePath();
          return new DataFileChannel(new File(path), offset, len);
        }
      }
      return null;
    }

    /**
     * Opens the block if it is not open.
     *
     * @param channel the netty channel
     * @throws Exception if it fails to open the block
     */
    private void openBlock(BlockReadRequestContext context, Channel channel) throws Exception {
      if (context.getBlockReader() != null) {
        return;
      }
      BlockReadRequest request = context.getRequest();
      int retryInterval = Constants.SECOND_MS;
      RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);

      // TODO(calvin): Update the locking logic so this can be done better
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

      do {
        try {
          BlockReader reader = mWorker.createBlockReader(request.getSessionId(), request.getId(),
              request.getStart(), false, request.getOpenUfsBlockOptions());
          String metricName = "BytesReadAlluxio";
          context.setBlockReader(reader);
          context.setCounter(MetricsSystem.counter(metricName));
          mWorker.accessBlock(request.getSessionId(), request.getId());
          if (reader.getChannel() instanceof FileChannel) {
            ((FileChannel) reader.getChannel()).position(request.getStart());
          }
          return;
        } catch (Exception e) {
          throw e;
        }

        /*
        ProtoMessage heartbeat = new ProtoMessage(Protocol.ReadResponse.newBuilder()
            .setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT).build());
        // Sends an empty buffer to the client to make sure that the client does not timeout when
        // the server is waiting for the UFS block access.
        channel.writeAndFlush(new RPCProtoMessage(heartbeat));
        */
      } while (retryPolicy.attempt());
    }
  }

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService  the executor service to run {@link PacketReader}s
   * @param blockWorker      the block worker
   * @param fileTransferType the file transfer type
   */
  public BlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
                          FileTransferType fileTransferType) {
    super(executorService);
    mWorker = blockWorker;
    mTransferType = fileTransferType;
  }

  @Override
  protected BlockReadRequestContext createRequestContext(Protocol.ReadRequest request) {
    return new BlockReadRequestContext(request);
  }

  @Override
  protected PacketReader createPacketReader(BlockReadRequestContext context, Channel channel) {
    return new BlockPacketReader(context, channel, mWorker);
  }
}
