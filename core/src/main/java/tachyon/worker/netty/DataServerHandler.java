package tachyon.worker.netty;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.channel.DefaultFileRegion;
import org.apache.log4j.Logger;

import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;
import tachyon.worker.BlocksLocker;

import com.google.common.io.Closer;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public final class DataServerHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = Logger.getLogger(DataServerHandler.class);

  private final BlocksLocker LOCKER;

  public DataServerHandler(BlocksLocker locker) {
    LOCKER = locker;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    // pipeline will make sure this is true
    final BlockRequest req = (BlockRequest) msg;

    final long blockId = req.getBlockId();
    final long offset = req.getOffset();
    final long len = req.getLength();

    int lockId = LOCKER.lock(blockId);

    try {
      if (offset < 0) {
        throw new IOException("Offset can not be negative: " + offset);
      }
      if (len < 0 && len != -1) {
        throw new IOException("Length can not be negative except -1: " + len);
      }

      String filePath = CommonUtils.concat(WorkerConf.get().DATA_FOLDER, blockId);
      LOG.info("Try to response remote request by reading from " + filePath);

      final Closer closer = Closer.create();
      try {
        RandomAccessFile file = closer.register(new RandomAccessFile(filePath, "r"));
        long fileLength = file.length();
        String error = null;
        if (offset > fileLength) {
          error = String.format("Offset(%d) is larger than file length(%d)", offset, fileLength);
        }
        if (error == null && len != -1 && offset + len > fileLength) {
          error =
              String.format("Offset(%d) plus length(%d) is larger than file length(%d)", offset,
                  len, fileLength);
        }
        if (error != null) {
          file.close();
          throw new IOException(error);
        }

        final long readLength;
        if (len == -1) {
          readLength = fileLength - offset;
        } else {
          readLength = len;
        }

        ctx.write(new BlockResponse(blockId, offset, readLength));
        FileChannel channel = closer.register(file.getChannel());
        ChannelFuture future = ctx.writeAndFlush(new DefaultFileRegion(channel, offset, readLength));
        future.addListener(ChannelFutureListener.CLOSE);
      } finally {
        closer.close();
      }
      LOG.info("Response remote request by reading from " + filePath + " preparation done.");
    } catch (Exception e) {
      // TODO This is a trick for now. The data may have been removed before remote retrieving.
      LOG.error("The file is not here : " + e.getMessage(), e);
      BlockResponse resp = new BlockResponse(-blockId, 0, 0);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
    } finally {
      LOCKER.unlock(Math.abs(blockId), lockId);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
