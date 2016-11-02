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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.DataTransferException;
import alluxio.metrics.MetricsSystem;
import alluxio.network.netty.MessageQueue;
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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s.
 */
// TODO(peis): We should not throw runtime exception in this class. Fix it.
@NotThreadSafe
final public class BlockReadDataServerHandler
    extends SimpleChannelInboundHandler<RPCBlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final Exception BLOCK_READ_CANCEL_EXCEPTION =
      new DataTransferException("Block read is cancelled.");

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  private volatile ChannelHandlerContext mContext = null;
  private volatile Long mBlockId = null;
  private volatile BlockReader mBlockReader = null;

  private static final int PACKET_SIZE = 64 * 1024;

  public final class ResponseQueue extends MessageQueue<RPCBlockReadResponse> {
    public ResponseQueue() {
      super(4);
    }

    @Override
    protected void signal() {
      mPacketReader.activate();
    }
  }

  // TODO(now): init these.
  private static final ExecutorService PACKET_READERS = null;
  private static final ExecutorService PACKET_WRITERS = null;

  public enum Status {
    ACTIVE, BLOCKED, STOPPING, DONE,
  }

  private volatile PacketReader mPacketReader = null;
  private volatile PacketWriter mPacketWriter = null;
  private ResponseQueue mResponseQueue = new ResponseQueue();

  private class PacketReader implements Runnable {
    private final RPCBlockReadRequest mRequest;

    @GuardedBy("this")
    private Status mStatus = Status.ACTIVE;

    public PacketReader(RPCBlockReadRequest request) {
      mRequest = request;
    }

    public synchronized void waitFor(Status status) {
      try {
        while (mStatus != status) {
          wait();
        }
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public synchronized void waitFor2(Status status1, Status status2) {
      try {
        while (mStatus != status1 && mStatus != status2) {
          wait();
        }
      } catch (InterruptedException e) {
        Throwables.propagate(e);
      }
    }

    public synchronized void stop() {
      if (mStatus == Status.DONE) {
        return;
      }
      mResponseQueue.exceptionCaught(BLOCK_READ_CANCEL_EXCEPTION);
      mStatus = Status.STOPPING;
      notify();
    }

    public synchronized void activate() {
      if (mStatus != Status.BLOCKED) {
        return;
      }
      mStatus = Status.ACTIVE;
      notify();
    }

    public synchronized void toBlocked() {
      if (mStatus == Status.ACTIVE) {
        mStatus = Status.BLOCKED;
      }
    }

    public synchronized boolean done() {
      return mStatus == Status.DONE;
    }

    @Override
    public void run() {
      try {
        if (mBlockId == null || mBlockId != mRequest.getBlockId()) {
          mBlockId = mRequest.getBlockId();
          if (mBlockReader != null) {
            mBlockReader.close();
          }
          mBlockReader = mWorker.readBlockRemote(mRequest.getSessionId(), mRequest.getBlockId(),
              mRequest.getLockId());
          mWorker.accessBlock(mRequest.getSessionId(), mRequest.getBlockId());
        }

        mRequest.validate();
        long blockLength = mBlockReader.getLength();
        validateBounds(mRequest, blockLength);

        long offset = mRequest.getOffset();
        long readLength = returnLength(offset, mRequest.getLength(), blockLength);
        while (readLength > 0) {
          // This can block. If the number of threads becomes a concern, we should consider
          // avoiding consuming a thread while waiting.
          waitFor2(Status.ACTIVE, Status.STOPPING);
          synchronized (this) {
            if (mStatus == Status.STOPPING) {
              break;
            }
          }
          toBlocked();

          int packet_size = (int) Math.min(readLength, (long) PACKET_SIZE);
          boolean isLastPacket = readLength <= PACKET_SIZE;
          DataBuffer packet = getDataBuffer(offset, packet_size);
          RPCBlockReadResponse response =
              new RPCBlockReadResponse(mBlockId, offset, packet_size, packet,
                  isLastPacket ? RPCResponse.Status.SUCCESS : RPCResponse.Status.STREAM_PACKET);
          mResponseQueue.offerMessage(response, isLastPacket);
          readLength -= PACKET_SIZE;
          offset += PACKET_SIZE;
        }
      } catch (IOException | AlluxioException e) {
        mResponseQueue.exceptionCaught(e);
      }
      synchronized (this) {
        mStatus = Status.DONE;
        notifyAll();
      }
    }
  }

  private class PacketWriter implements Runnable {
    @GuardedBy("this")
    private boolean mDone = false;

    private RPCBlockReadRequest mRequest;

    public PacketWriter(RPCBlockReadRequest request) {
      mRequest = request;
    }

    @Override
    public void run() {
      RPCResponse response;

      boolean done = false;
      do {
        try {
          response = mResponseQueue.pollMessage();
          if (response.getStatus() == RPCResponse.Status.SUCCESS) {
            done = true;
          }
        } catch (Throwable e) {
          if (e instanceof BlockDoesNotExistException) {
            response = RPCBlockReadResponse
                .createErrorResponse(mRequest.getBlockId(), RPCResponse.Status.FILE_DNE);
          } else {
            response = RPCBlockReadResponse
                .createErrorResponse(mRequest.getBlockId(), RPCResponse.Status.UFS_READ_FAILED);
          }
          done = true;
        }
        try {
          // TODO(peis): Investigate whether we should make use of the netty outbound buffer here.
          ChannelFuture channelFuture = mContext.writeAndFlush(response).sync();
          channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
          if (!channelFuture.isSuccess()) {
            Preconditions.checkNotNull(channelFuture.cause());
            mResponseQueue.exceptionCaught(channelFuture.cause());
          }
        } catch (InterruptedException e) {
          Throwables.propagate(e);
        }

        if (done) {
          synchronized (this) {
            mDone = true;
            notify();
            break;
          }
        }
      } while (true);
    }

    public synchronized boolean done() {
      return mDone;
    }

    public synchronized void waitForDone() {
      while (!mDone) {
        try {
          wait();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  public BlockReadDataServerHandler(BlockWorker worker, FileTransferType transferType) {
    mWorker = worker;
    mTransferType = transferType;
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) {
    if (mPacketReader != null) {
      mPacketReader.stop();
      mPacketReader.waitFor(Status.DONE);
    }
    if (mPacketWriter != null) {
      mPacketWriter.waitForDone();
    }
    try {
      mBlockReader.close();
    } catch (IOException e) {
      LOG.error("Failed to close block reader in channelInactive() call.", e);
      // Cannot do anything here. The exception is ignored.
    }

    mResponseQueue.reset();
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockReadRequest msg) {
    if (mContext != null) {
      mContext = ctx;
    }
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
