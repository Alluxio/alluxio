package tachyon.worker.netty.handler;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.log4j.Logger;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import tachyon.Constants;
import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;
import tachyon.worker.BlocksLocker;
import tachyon.worker.netty.ClosableResourceChannelListener;
import tachyon.worker.netty.protocol.GetBlock;
import tachyon.worker.netty.protocol.GetBlockResponse;

public final class GetBlockHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  private final BlocksLocker mLocker;

  public GetBlockHandler(BlocksLocker mLocker) {
    this.mLocker = mLocker;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof GetBlock) {
      GetBlock getBlock = (GetBlock) msg;
      final long blockId = getBlock.getBlockId();

      final int lockId = mLocker.lock(blockId);
      try {
        String filePath = CommonUtils.concat(WorkerConf.get().DATA_FOLDER, blockId);
        LOG.info("Try to response remote request by reading from " + filePath);

        RandomAccessFile file = new RandomAccessFile(filePath, "r");
        long fileLength = file.length();
        if (inRange(getBlock, fileLength)) {
          final long readLength = returnLength(getBlock.getOffset(), getBlock.getLength(), fileLength);

          FileChannel channel = file.getChannel();
          ChannelFuture future =
              ctx.writeAndFlush(new GetBlockResponse(blockId, getBlock.getOffset(), readLength, channel));

          future.addListener(ChannelFutureListener.CLOSE);
          future.addListener(new ClosableResourceChannelListener(file));
          LOG.info("Response remote request by reading from " + filePath + " preparation done.");
        } else {
          // unable to read data, range check fail
        }
      } finally {
        mLocker.unlock(blockId, lockId);
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    ctx.close().sync();
    cause.printStackTrace();
  }

  /**
   * Returns how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    if (len == -1) {
      return fileLength - offset;
    } else {
      return len;
    }
  }

  private static boolean inRange(GetBlock msg, long fileSize) {
    // offset is greater than file size
    if (msg.getOffset() > fileSize) {
      return false;
    }
    // offset + length is larger than the file
    if (msg.hasLength() && msg.getOffset() + msg.getLength() > fileSize) {
      return false;
    }
    return true;
  }
}
