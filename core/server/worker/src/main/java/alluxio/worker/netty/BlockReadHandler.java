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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.UnderFileSystemBlockReader;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles block read request. Check more information in
 * {@link AbstractReadHandler}.
 */
@NotThreadSafe
final class BlockReadHandler extends AbstractReadHandler<BlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS = Configuration.getMs(
      PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** The Block Worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   * @param blockWorker the block worker
   * @param fileTransferType the file transfer type
   */
  public BlockReadHandler(ExecutorService executorService, BlockWorker blockWorker,
      FileTransferType fileTransferType) {
    super(executorService);
    mWorker = blockWorker;
    mTransferType = fileTransferType;
  }

  @Override
  protected BlockReadRequest createRequest(Protocol.ReadRequest request) throws Exception {
    return new BlockReadRequest(request);
  }

  @Override
  protected void completeRequest() throws Exception {
    BlockReadRequest request = getRequest();
    request.getContext().getBlockReader().close();
    if (!mWorker.unlockBlock(request.getSessionId(), request.getId())) {
      mWorker.closeUfsBlock(request.getSessionId(), request.getId());
    }
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws Exception {
    openBlock(channel);
    BlockReader blockReader = getRequest().getContext().getBlockReader();
    if (mTransferType == FileTransferType.TRANSFER
        && (blockReader instanceof LocalFileBlockReader)) {
      return new DataFileChannel(new File(((LocalFileBlockReader) blockReader).getFilePath()),
          offset, len);
    } else {
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
  }

  /**
   * Opens the block if it is not open.
   *
   * @param channel the netty channel
   * @throws Exception if it fails to open the block
   */
  private void openBlock(Channel channel) throws Exception {
    BlockReadRequest request = getRequest();
    int retryInterval = Constants.SECOND_MS;
    RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);

    // TODO(calvin): Update the locking logic so this can be done better
    if (request.isPromote()) {
      try {
        mWorker.moveBlock(request.getSessionId(), request.getId(), mStorageTierAssoc.getAlias(0));
      } catch (BlockDoesNotExistException e) {
        LOG.debug(
            "Block {} to promote does not exist in Alluxio: {}", request.getId(), e.getMessage());
      } catch (Exception e) {
        LOG.warn("Failed to promote block {}: {}", request.getId(), e.getMessage());
      }
    }

    do {
      long lockId;
      if (request.isPersisted()) {
        lockId = mWorker.lockBlockNoException(request.getSessionId(), request.getId());
      } else {
        lockId = mWorker.lockBlock(request.getSessionId(), request.getId());
      }
      if (lockId != BlockLockManager.INVALID_LOCK_ID) {
        try {
          BlockReadRequest.Context context = request.getContext();
          context.setBlockReader(
              mWorker.readBlockRemote(request.getSessionId(), request.getId(), lockId));
          context.setCounter(MetricsSystem.workerCounter("BytesReadAlluxio"));
          mWorker.accessBlock(request.getSessionId(), request.getId());
          ((FileChannel) context.getBlockReader().getChannel()).position(request.getStart());
          return;
        } catch (Exception e) {
          mWorker.unlockBlock(lockId);
          throw e;
        }
      }

      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
      if (mWorker.openUfsBlock(request.getSessionId(), request.getId(), openUfsBlockOptions)) {
        try {
          BlockReader reader = mWorker
              .readUfsBlock(request.getSessionId(), request.getId(), request.getStart());
          AlluxioURI ufsMountPointUri =
              ((UnderFileSystemBlockReader) reader).getUfsMountPointUri();
          String ufsString = MetricsSystem.escape(ufsMountPointUri);
          String metricName = String.format("BytesReadUfs-Ufs:%s", ufsString);
          BlockReadRequest.Context context = request.getContext();
          context.setBlockReader(reader);
          context.setCounter(MetricsSystem.workerCounter(metricName));
          return;
        } catch (Exception e) {
          mWorker.closeUfsBlock(request.getSessionId(), request.getId());
          throw e;
        }
      }

      ProtoMessage heartbeat = new ProtoMessage(
          Protocol.ReadResponse.newBuilder().setType(Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT)
              .build());
      // Sends an empty buffer to the client to make sure that the client does not timeout when
      // the server is waiting for the UFS block access.
      channel.writeAndFlush(new RPCProtoMessage(heartbeat));
    } while (retryPolicy.attemptRetry());
    throw new UnavailableException(ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE
        .getMessage(request.getId(), request.getOpenUfsBlockOptions().getUfsPath()));
  }

  @Override
  protected void incrementMetrics(long bytesRead) {
    getRequest().getContext().getCounter();
  }
}
