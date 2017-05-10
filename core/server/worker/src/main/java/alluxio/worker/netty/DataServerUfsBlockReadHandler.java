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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnavailableException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.worker.block.BlockLockManager;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.UnderFileSystemBlockReader;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This handler handles UFS block read request. Check more information in
 * {@link DataServerReadHandler}.
 */
@NotThreadSafe
final class DataServerUfsBlockReadHandler extends DataServerReadHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerUfsBlockReadHandler.class);
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS = Configuration.getMs(
      PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** The Block Worker. */
  private final BlockWorker mWorker;

  /**
   * The block read request internal representation.
   */
  private final class UfsBlockReadRequestInternal extends ReadRequestInternal {
    BlockReader mBlockReader;
    final Protocol.OpenUfsBlockOptions mOpenUfsBlockOptions;
    final boolean mNoCache;

    /**
     * Creates an instance of {@link UfsBlockReadRequestInternal}.
     *
     * @param request the block read request
     */
    UfsBlockReadRequestInternal(Protocol.ReadRequest request) throws Exception {
      super(request.getSessionId(), request.getId(), request.getOffset(),
          request.getOffset() + request.getLength(), request.getPacketSize());

      mOpenUfsBlockOptions = request.getOpenUfsBlockOptions();
      mNoCache = request.getNoCache();
      // Note that we do not need to seek to offset since the block worker is created at the offset.
    }

    @Override
    public void close() {
      if (mBlockReader != null)  {
        try {
          mBlockReader.close();
       } catch (Exception e) {
          LOG.warn("Failed to close block reader for block {} with error {}.", mId, e.getMessage());
        }
      }

      try {
        if (mBlockReader instanceof UnderFileSystemBlockReader) {
          mWorker.closeUfsBlock(mRequest.mSessionId, mRequest.mId);
        } else {
          mWorker.unlockBlock(mRequest.mSessionId, mRequest.mId);
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
    Protocol.ReadRequest request = ((RPCProtoMessage) object).getMessage().asReadRequest();
    return request.getType() == Protocol.RequestType.UFS_BLOCK;
  }

  @Override
  protected void initializeRequest(Protocol.ReadRequest request) throws Exception {
    mRequest = new UfsBlockReadRequestInternal(request);
  }

  @Override
  protected DataBuffer getDataBuffer(Channel channel, long offset, int len) throws Exception {
    openUfsBlock(channel);
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

  /**
   * Opens the UFS block if it is not open.
   *
   * @param channel the netty channel
   * @throws Exception if it fails to open the UFS block
   */
  private void openUfsBlock(Channel channel) throws Exception {
    UfsBlockReadRequestInternal request = (UfsBlockReadRequestInternal) mRequest;
    if (request.mBlockReader != null) {
      return;
    }
    int retryInterval = Constants.SECOND_MS;
    RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, retryInterval);

    do {
      long lockId = mWorker.lockBlockNoException(request.mSessionId, request.mId);
      if (lockId != BlockLockManager.INVALID_LOCK_ID) {
        try {
          request.mBlockReader = mWorker.readBlockRemote(request.mSessionId, request.mId, lockId);
        } catch (Exception e) {
          mWorker.unlockBlock(lockId);
          throw e;
        }
        break;
      }

      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      if (mWorker.openUfsBlock(request.mSessionId, request.mId, request.mOpenUfsBlockOptions)) {
        try {
          request.mBlockReader = mWorker
              .readUfsBlock(request.mSessionId, request.mId, request.mStart, request.mNoCache);
        } catch (Exception e) {
          mWorker.closeUfsBlock(request.mSessionId, request.mId);
          throw e;
        }
        break;
      }

      // Sends an empty buffer to the client to make sure that the client does not timeout when
      // the server is waiting for the UFS block access.
      channel.writeAndFlush(
          RPCProtoMessage.createOkResponse(new DataNettyBufferV2(channel.alloc().buffer(0, 0))));
    } while (retryPolicy.attemptRetry());
    throw new UnavailableException(ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE
        .getMessage(request.mId, request.mOpenUfsBlockOptions.getUfsPath()));
  }

  @Override
  protected void incrementMetrics(long bytesRead) {
    if (((UfsBlockReadRequestInternal) mRequest).mBlockReader instanceof
        UnderFileSystemBlockReader) {
      Metrics.BYTES_READ_UFS.inc(bytesRead);
    } else {
      Metrics.BYTES_READ_REMOTE.inc(bytesRead);
    }
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_UFS = MetricsSystem.workerCounter("BytesReadUFS");
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.workerCounter("BytesReadRemote");

    private Metrics() {
    } // prevent instantiation
  }
}
