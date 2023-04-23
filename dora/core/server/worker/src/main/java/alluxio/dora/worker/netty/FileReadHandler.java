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

package alluxio.dora.worker.netty;

import alluxio.dora.Constants;
import alluxio.dora.conf.Configuration;
import alluxio.dora.conf.PropertyKey;
import alluxio.dora.metrics.MetricsSystem;
import alluxio.dora.network.netty.FileTransferType;
import alluxio.dora.network.protocol.databuffer.DataBuffer;
import alluxio.dora.network.protocol.databuffer.DataFileChannel;
import alluxio.dora.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.dora.retry.RetryPolicy;
import alluxio.dora.retry.TimeoutRetry;
import alluxio.dora.worker.block.io.BlockReader;
import alluxio.dora.worker.block.io.LocalFileBlockReader;
import alluxio.dora.worker.dora.DoraWorker;

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
 * Handles file read request.
 */
public class FileReadHandler extends AbstractReadHandler<BlockReadRequestContext> {
  private static final Logger LOG = LoggerFactory.getLogger(FileReadHandler.class);

  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  private final DoraWorker mWorker;

  /**
   * The transfer type used by the data server.
   */
  private final FileTransferType mTransferType;

  /**
   * Creates an instance of {@link FileReadHandler}.
   *
   * @param executorService the executor service to run data readers
   * @param worker block worker
   * @param fileTransferType the file transfer type
   */
  public FileReadHandler(ExecutorService executorService,
                         DoraWorker worker, FileTransferType fileTransferType) {
    super(executorService);
    mWorker = worker;
    mTransferType = fileTransferType;
  }

  @Override
  protected BlockReadRequestContext createRequestContext(Protocol.ReadRequest request) {
    return new BlockReadRequestContext(request);
  }

  @Override
  protected AbstractReadHandler<BlockReadRequestContext>.PacketReader createPacketReader(
      BlockReadRequestContext context, Channel channel) {
    return new BlockPacketReader(context, channel, mWorker);
  }

  /**
   * The packet reader to read from a local block worker.
   */
  @NotThreadSafe
  public final class BlockPacketReader extends PacketReader {
    /**
     * The Block Worker.
     */
    private final DoraWorker mWorker;

    BlockPacketReader(BlockReadRequestContext context, Channel channel, DoraWorker worker) {
      super(context, channel);
      mWorker = worker;
    }

    @Override
    protected void completeRequest(BlockReadRequestContext context) throws Exception {
      BlockReader reader = context.getBlockReader();
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {} with error {}.",
              context.getRequest(), e.getMessage());
        }
      }
    }

    @Override
    protected DataBuffer getDataBuffer(BlockReadRequestContext context, Channel channel,
                                       long offset, int len) throws Exception {
      openBlock(context, channel);
      BlockReader blockReader = context.getBlockReader();
      Preconditions.checkState(blockReader != null);
      if (mTransferType == FileTransferType.TRANSFER
          && (blockReader instanceof LocalFileBlockReader)) {
        return new DataFileChannel(new File(((LocalFileBlockReader) blockReader).getFilePath()),
            offset, len);
      } else {
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
      ReadRequest readRequest = context.getRequest();
      if (readRequest instanceof BlockReadRequest == false) {
        throw new UnsupportedOperationException("Cast exception from " + readRequest.getClass()
            + " to " + BlockReadRequest.class);
      }
      BlockReadRequest blockReadRequest = (BlockReadRequest) readRequest;
      int retryInterval = Constants.SECOND_MS;
      RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);
      do {
        try {
          BlockReader reader =
              mWorker.createFileReader(blockReadRequest.getOpenUfsBlockOptions().getUfsPath(),
              blockReadRequest.getStart(),
                  false, blockReadRequest.getOpenUfsBlockOptions());
          String metricName = "BytesReadAlluxio";
          context.setBlockReader(reader);
          context.setCounter(MetricsSystem.counter(metricName));
          if (reader.getChannel() instanceof FileChannel) {
            ((FileChannel) reader.getChannel()).position(blockReadRequest.getStart());
          }
          return;
        } catch (Exception e) {
          throw e;
        }

        //TODO(JiamingMai): Not sure if this is necessary.
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
}
