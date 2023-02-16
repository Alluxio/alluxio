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
import alluxio.metrics.MetricsSystem;
import alluxio.network.netty.FileTransferType;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;
import alluxio.worker.dora.DoraWorker;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   */
  public FileReadHandler(ExecutorService executorService, DoraWorker worker, FileTransferType fileTransferType) {
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

    //TODO(JiamingMai): implement this method
    return null;
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
    /**
     * An object storing the mapping of tier aliases to ordinals.
     */
    private final StorageTierAssoc mStorageTierAssoc = new DefaultStorageTierAssoc(
        PropertyKey.WORKER_TIERED_STORE_LEVELS,
        PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS);

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
              context.getRequest().getId(), e.getMessage());
        }
      }
      /*
      if (!mWorker.unlockBlock(context.getRequest().getSessionId(), context.getRequest().getId())) {
        if (reader != null) {
          mWorker.closeUfsBlock(context.getRequest().getSessionId(), context.getRequest().getId());
          context.setBlockReader(null);
        }
      }
      */
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
      alluxio.worker.netty.BlockReadRequest request = context.getRequest();
      int retryInterval = Constants.SECOND_MS;
      RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);
      do {
        try {
          BlockReader reader = mWorker.createFileReader(request.getOpenUfsBlockOptions().getUfsPath(),
              request.getStart(), false, request.getOpenUfsBlockOptions());
          String metricName = "BytesReadAlluxio";
          context.setBlockReader(reader);
          context.setCounter(MetricsSystem.counter(metricName));
          //TODO(JiamingMai): Do we still need to access block?
          // It seems that it is not necessary any more.
          //mWorker.accessBlock(request.getSessionId(), request.getId());
          if (reader.getChannel() instanceof FileChannel) {
            ((FileChannel) reader.getChannel()).position(request.getStart());
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