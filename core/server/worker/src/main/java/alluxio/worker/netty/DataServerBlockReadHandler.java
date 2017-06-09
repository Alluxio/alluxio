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

import com.codahale.metrics.Counter;
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
 * {@link DataServerReadHandler}.
 */
@NotThreadSafe
final class DataServerBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerBlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS = Configuration.getMs(
      PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** The Block Worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /**
   * The block read request internal representation. When this request is closed, it will clean
   * up any temporary state it may have accumulated.
   */
  private final class BlockReadRequestInternal extends ReadRequestInternal {
    BlockReader mBlockReader;
    Counter mCounter;
    final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
    final boolean mPromote;

    /**
     * Creates an instance of {@link BlockReadRequestInternal}.
     *
     * @param request the block read request
     */
    BlockReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      super(request.getBlockId(), request.getOffset(), request.getOffset() + request.getLength(),
          request.getPacketSize());

      if (request.hasOpenUfsBlockOptions()) {
        mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
      } else {
        mOpenUfsBlockOptions = null;
      }
      mPromote = request.getPromote();
      // Note that we do not need to seek to offset since the block worker is created at the offset.
    }

    /**
     * @return true if the block is persisted in UFS
     */
    boolean isPersisted() {
      return mOpenUfsBlockOptions != null && mOpenUfsBlockOptions.hasUfsPath();
    }

    @Override
    public void close() {
      if (mBlockReader != null) {
        try {
          mBlockReader.close();
        } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {} with error {}.", mId, e.getMessage());
        }
      }

      try {
        if (!mWorker.unlockBlock(mSessionId, mId)) {
          mWorker.closeUfsBlock(mSessionId, mId);
        }
      } catch (Exception e) {
        LOG.warn("Failed to unlock block {} with error {}.", mId, e.getMessage());
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
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new BlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws Exception {
    openBlock(channel);
    BlockReader blockReader = ((BlockReadRequestInternal) mRequest).mBlockReader;

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
    BlockReadRequestInternal request = (BlockReadRequestInternal) mRequest;
    if (request.mBlockReader != null) {
      return;
    }

    int retryInterval = Constants.SECOND_MS;
    RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);

    // TODO(calvin): Update the locking logic so this can be done better
    if (request.mPromote) {
      try {
        mWorker.moveBlock(request.mSessionId, request.mId, mStorageTierAssoc.getAlias(0));
      } catch (BlockDoesNotExistException e) {
        LOG.debug("Block {} to promote does not exist in Alluxio: {}", request.mId, e.getMessage());
      } catch (Exception e) {
        LOG.warn("Failed to promote block {}: {}", request.mId, e.getMessage());
      }
    }

    do {
      long lockId;
      if (request.isPersisted()) {
        lockId = mWorker.lockBlockNoException(request.mSessionId, request.mId);
      } else {
        lockId = mWorker.lockBlock(request.mSessionId, request.mId);
      }
      if (lockId != BlockLockManager.INVALID_LOCK_ID) {
        try {
          request.mBlockReader = mWorker.readBlockRemote(request.mSessionId, request.mId, lockId);
          request.mCounter = MetricsSystem.workerCounter("BytesReadAlluxio");
          mWorker.accessBlock(request.mSessionId, request.mId);
          ((FileChannel) request.mBlockReader.getChannel()).position(request.mStart);
          return;
        } catch (Exception e) {
          mWorker.unlockBlock(lockId);
          throw e;
        }
      }

      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.mOpenUfsBlockOptions;
      if (mWorker.openUfsBlock(request.mSessionId, request.mId, openUfsBlockOptions)) {
        try {
          request.mBlockReader = mWorker
              .readUfsBlock(request.mSessionId, request.mId, request.mStart);
          AlluxioURI ufsMountPointUri =
              ((UnderFileSystemBlockReader) request.mBlockReader).getUfsMountPointUri();
          String ufsString = MetricsSystem.escape(ufsMountPointUri);
          String metricName = String.format("BytesReadUfs-Ufs:%s", ufsString);
          request.mCounter = MetricsSystem.workerCounter(metricName);
          return;
        } catch (Exception e) {
          mWorker.closeUfsBlock(request.mSessionId, request.mId);
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
        .getMessage(request.mId, request.mOpenUfsBlockOptions.getUfsPath()));
  }

  @Override
  protected void incrementMetrics(long bytesRead) {
    Counter counter = ((BlockReadRequestInternal) mRequest).mCounter;
    if (counter == null) {
      throw new IllegalStateException("metric counter is null");
    }
    counter.inc(bytesRead);
  }
}
