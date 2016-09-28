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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s and {@link RPCBlockWriteRequest}s.
 */
@NotThreadSafe
final class BlockDataServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc;

  BlockDataServerHandler(BlockWorker worker) {
    mWorker = worker;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
    mTransferType = Configuration
        .getEnum(PropertyKey.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE, FileTransferType.class);
  }

  /**
   * Handles a {@link RPCBlockReadRequest} by reading the data through a {@link BlockReader}
   * provided by the block worker. This method assumes the data is available in the local storage
   * of the worker and returns an error status if the data is not available.
   *
   * @param ctx The context of this request which handles the result of this operation
   * @param req The initiating {@link RPCBlockReadRequest}
   * @throws IOException if an I/O error occurs when reading the data requested
   */
  void handleBlockReadRequest(final ChannelHandlerContext ctx, final RPCBlockReadRequest req)
      throws IOException {
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();
    final long lockId = req.getLockId();
    final long sessionId = req.getSessionId();

    BlockReader reader = null;
    DataBuffer buffer;
    try {
      req.validate();
      reader = mWorker.readBlockRemote(sessionId, blockId, lockId);
      final long fileLength = reader.getLength();
      validateBounds(req, fileLength);
      final long readLength = returnLength(offset, len, fileLength);
      buffer = getDataBuffer(req, reader, readLength);
      Metrics.BYTES_READ_REMOTE.inc(buffer.getLength());
      RPCBlockReadResponse resp =
          new RPCBlockReadResponse(blockId, offset, readLength, buffer, RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(new ClosableResourceChannelListener(reader));
      future.addListener(new ReleasableResourceChannelListener(buffer));
      mWorker.accessBlock(sessionId, blockId);
      LOG.debug("Preparation for responding to remote block request for: {} done.", blockId);
    } catch (Exception e) {
      LOG.error("Exception reading block {}", blockId, e);
      RPCBlockReadResponse resp;
      if (e instanceof BlockDoesNotExistException) {
        resp = RPCBlockReadResponse.createErrorResponse(req, RPCResponse.Status.FILE_DNE);
      } else {
        resp = RPCBlockReadResponse.createErrorResponse(req, RPCResponse.Status.UFS_READ_FAILED);
      }
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Handles a {@link RPCBlockWriteRequest} by writing the data through a {@link BlockWriter}
   * provided by the block worker. This method takes care of requesting space and creating the
   * block if necessary.
   *
   * @param ctx The context of this request which handles the result of this operation
   * @param req The initiating {@link RPCBlockWriteRequest}
   * @throws IOException if an I/O exception occurs when writing the data
   */
  // TODO(hy): This write request handler is very simple in order to be stateless. Therefore, the
  // block file is opened and closed for every request. If this is too slow, then this handler
  // should be optimized to keep state.
  void handleBlockWriteRequest(final ChannelHandlerContext ctx, final RPCBlockWriteRequest req)
      throws IOException {
    final long sessionId = req.getSessionId();
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long length = req.getLength();
    final DataBuffer data = req.getPayloadDataBuffer();

    BlockWriter writer = null;
    try {
      req.validate();
      ByteBuffer buffer = data.getReadOnlyByteBuffer();

      if (offset == 0) {
        // This is the first write to the block, so create the temp block file. The file will only
        // be created if the first write starts at offset 0. This allocates enough space for the
        // write.
        mWorker.createBlockRemote(sessionId, blockId, mStorageTierAssoc.getAlias(0), length);
      } else {
        // Allocate enough space in the existing temporary block for the write.
        mWorker.requestSpace(sessionId, blockId, length);
      }
      writer = mWorker.getTempBlockWriterRemote(sessionId, blockId);
      writer.append(buffer);

      Metrics.BYTES_WRITTEN_REMOTE.inc(data.getLength());
      RPCBlockWriteResponse resp =
          new RPCBlockWriteResponse(sessionId, blockId, offset, length, RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(new ClosableResourceChannelListener(writer));
    } catch (Exception e) {
      LOG.error("Error writing remote block : {}", e.getMessage(), e);
      RPCBlockWriteResponse resp =
          RPCBlockWriteResponse.createErrorResponse(req, RPCResponse.Status.WRITE_ERROR);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (writer != null) {
        writer.close();
      }
    }
  }

  /**
   * @return how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    return (len == -1) ? fileLength - offset : len;
  }

  /**
   * Validates the bounds of the request. An uncaught exception will be thrown if an
   * inconsistency occurs.
   *
   * @param req The initiating {@link RPCBlockReadRequest}
   * @param fileLength The length of the block being read
   */
  private void validateBounds(final RPCBlockReadRequest req, final long fileLength) {
    Preconditions.checkArgument(req.getOffset() <= fileLength,
        "Offset(%s) is larger than file length(%s)", req.getOffset(), fileLength);
    Preconditions.checkArgument(
        req.getLength() == -1 || req.getOffset() + req.getLength() <= fileLength,
        "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
        req.getLength(), fileLength);
  }

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param req The initiating {@link RPCBlockReadRequest}
   * @param reader The {@link BlockReader} for the block to read
   * @param readLength The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   * @throws IOException if an I/O error occurs when reading the data
   */
  private DataBuffer getDataBuffer(RPCBlockReadRequest req, BlockReader reader, long readLength)
      throws IOException, IllegalArgumentException {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = reader.read(req.getOffset(), (int) readLength);
        return new DataByteBuffer(data, readLength);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        if (reader.getChannel() instanceof FileChannel) {
          return new DataFileChannel((FileChannel) reader.getChannel(), req.getOffset(),
              readLength);
        }
        reader.close();
        throw new IllegalArgumentException("Only FileChannel is supported!");
    }
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.workerCounter("BytesReadRemote");
    private static final Counter BYTES_WRITTEN_REMOTE =
        MetricsSystem.workerCounter("BytesWrittenRemote");

    private Metrics() {} // prevent instantiation
  }
}
