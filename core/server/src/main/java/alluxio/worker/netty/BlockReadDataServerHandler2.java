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
import alluxio.exception.AlluxioException;
import alluxio.exception.DataTransferException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s.
 */
@NotThreadSafe
final public class BlockReadDataServerHandler2
    extends SimpleChannelInboundHandler<RPCBlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final Exception BLOCK_READ_CANCEL_EXCEPTION =
      new DataTransferException("Block read is cancelled.");

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  private volatile long mBlockId = 0;
  private volatile BlockReader mBlockReader = null;

  private static final int PACKET_SIZE = 64 * 1024;

  // TODO(now): init these.
  private static final ExecutorService PACKET_READERS = null;

  private class PacketReader implements Runnable {
    private final RPCBlockReadRequest mRequest;


  }

  public BlockReadDataServerHandler2(BlockWorker worker, FileTransferType transferType) {
    mWorker = worker;
    mTransferType = transferType;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockReadRequest msg) {

    // 1. init
    // 2. validate
    // 3. process cancel request.
    // 4. process normal request. Start packet reader.

    // Very similar as block writer implementation.

    // Packet reader.
    // Read a packet and write to the netty buffer. If buffer is full, pause.

    if (msg.isCancelRequest()) {
      mPacketReader.stop();
    } else {
      Preconditions.checkState(mPacketReader.done() && mPacketWriter.done(),
          "Block read request {} received on a busy channel.", msg);
      mResponseQueue.reset();
      mPacketReader = new PacketReader(msg);
      mPacketWriter = new PacketWriter(msg);
      PACKET_READERS.submit(mPacketReader);
      PACKET_WRITERS.submit(mPacketWriter);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught {} in BlockReadDataServerHandler.", cause);
    // This is likely a read error, close the channel to be safe.
    ctx.close();
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
    Preconditions
        .checkArgument(req.getOffset() <= fileLength, "Offset(%s) is larger than file length(%s)",
            req.getOffset(), fileLength);
    Preconditions
        .checkArgument(req.getLength() == -1 || req.getOffset() + req.getLength() <= fileLength,
            "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
            req.getLength(), fileLength);
  }

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param len The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   * @throws IOException if an I/O error occurs when reading the data
   */
  private DataBuffer getDataBuffer(long offset, int len)
      throws IOException, IllegalArgumentException {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = mBlockReader.read(offset, len);
        return new DataByteBuffer(data, len);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        Preconditions.checkArgument(mBlockReader.getChannel() instanceof FileChannel,
            "Only FileChannel is supported!");
        return new DataFileChannel((FileChannel) mBlockReader.getChannel(), offset, len);
    }
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.workerCounter("BytesReadRemote");
    private static final Counter BYTES_WRITTEN_REMOTE =
        MetricsSystem.workerCounter("BytesWrittenRemote");

    private Metrics() {
    } // prevent instantiation
  }
}
