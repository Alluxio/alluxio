package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
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

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class BlockDataServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final BlockWorker mWorker;
  private final FileTransferType mTransferType;
  private final StorageTierAssoc mStorageTierAssoc;

  protected BlockDataServerHandler(BlockWorker worker, Configuration configuration) {
    mWorker = worker;
    mStorageTierAssoc = new WorkerStorageTierAssoc(configuration);
    mTransferType = configuration.getEnum(Constants.WORKER_NETWORK_NETTY_FILE_TRANSFER_TYPE,
        FileTransferType.class);
  }

  protected void handleBlockReadRequest(final ChannelHandlerContext ctx,
      final RPCBlockReadRequest req) throws IOException {
    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();
    final long lockId = req.getLockId();
    final long sessionId = req.getSessionId();

    BlockReader reader;
    try {
      reader = mWorker.readBlockRemote(sessionId, blockId, lockId);
    } catch (BlockDoesNotExistException e) {
      throw new IOException(e);
    } catch (InvalidWorkerStateException e) {
      throw new IOException(e);
    }
    try {
      req.validate();
      final long fileLength = reader.getLength();
      validateBounds(req, fileLength);
      final long readLength = returnLength(offset, len, fileLength);
      RPCBlockReadResponse resp = new RPCBlockReadResponse(blockId, offset, readLength,
          getDataBuffer(req, reader, readLength), RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(reader));
      mWorker.accessBlock(sessionId, blockId);
      LOG.info("Preparation for responding to remote block request for: {} done.", blockId);
    } catch (Exception e) {
      LOG.error("The file is not here : {}", e.getMessage(), e);
      RPCBlockReadResponse resp =
          RPCBlockReadResponse.createErrorResponse(req, RPCResponse.Status.FILE_DNE);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (reader != null) {
        reader.close();
      }
    }
  }

  // TODO(hy): This write request handler is very simple in order to be stateless. Therefore, the
  // block file is opened and closed for every request. If this is too slow, then this handler
  // should be optimized to keep state.
  protected void handleBlockWriteRequest(final ChannelHandlerContext ctx,
      final RPCBlockWriteRequest req) throws IOException {
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

      RPCBlockWriteResponse resp =
          new RPCBlockWriteResponse(sessionId, blockId, offset, length, RPCResponse.Status.SUCCESS);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
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
   * Returns how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    return (len == -1) ? fileLength - offset : len;
  }

  private void validateBounds(final RPCBlockReadRequest req, final long fileLength) {
    Preconditions.checkArgument(req.getOffset() <= fileLength,
        "Offset(%s) is larger than file length(%s)", req.getOffset(), fileLength);
    Preconditions.checkArgument(
        req.getLength() == -1 || req.getOffset() + req.getLength() <= fileLength,
        "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
        req.getLength(), fileLength);
  }

  /**
   * Returns the appropriate {@link alluxio.network.protocol.databuffer.DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param req The initiating {@link RPCBlockReadRequest}
   * @param reader The {@link BlockReader} for the block to read
   * @param readLength The length, in bytes, of the data to read from the block
   * @return a {@link alluxio.network.protocol.databuffer.DataBuffer} representing the data
   * @throws IOException
   * @throws IllegalArgumentException
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
}
