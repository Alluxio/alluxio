/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.netty;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import tachyon.conf.WorkerConf;
import tachyon.util.CommonUtils;
import tachyon.worker.BlocksLocker;

import com.google.common.io.Closeables;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.DefaultFileRegion;
import io.netty.channel.socket.nio.NioSocketChannel;

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

    RandomAccessFile file = null;
    try {
      if (offset < 0) {
        throw new IOException("Offset can not be negative: " + offset);
      }
      if (len < 0 && len != -1) {
        throw new IOException("Length can not be negative except -1: " + len);
      }

      String filePath = CommonUtils.concat(WorkerConf.get().DATA_FOLDER, blockId);
      LOG.info("Try to response remote request by reading from " + filePath);

      file = new RandomAccessFile(filePath, "r");
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
        throw new IOException(error);
      }

      final long readLength;
      if (len == -1) {
        readLength = fileLength - offset;
      } else {
        readLength = len;
      }

      ChannelFuture future = null;
      FileChannel channel = file.getChannel();
//      if (ctx.channel().getClass().equals(NioSocketChannel.class)) {
//        ctx.write(new BlockResponse(blockId, offset, readLength));
//        future = ctx.writeAndFlush(new DefaultFileRegion(channel, offset, readLength));
//      } else {
//        // only nio supports FileRegion, so read data here
//        MappedByteBuffer data = channel.map(FileChannel.MapMode.READ_ONLY, offset, readLength);
//        future = ctx.writeAndFlush(new BlockResponse(blockId, offset, readLength, data));
//      }
      ctx.write(new BlockResponse(blockId, offset, readLength));
      future = ctx.writeAndFlush(new DefaultFileRegion(channel, offset, readLength));
      future.addListener(ChannelFutureListener.CLOSE);
      future.addListener(new ClosableResourceChannelListener(file));
      LOG.info("Response remote request by reading from " + filePath + " preparation done.");
    } catch (Exception e) {
      // TODO This is a trick for now. The data may have been removed before remote retrieving.
      LOG.error("The file is not here : " + e.getMessage(), e);
      BlockResponse resp = new BlockResponse(-blockId, 0, 0);
      ChannelFuture future = ctx.writeAndFlush(resp);
      future.addListener(ChannelFutureListener.CLOSE);
      if (file != null) {
        Closeables.closeQuietly(file);
      }
    } finally {
      LOCKER.unlock(Math.abs(blockId), lockId);
    }
  }

  private static final class ClosableResourceChannelListener implements ChannelFutureListener {
    private final Closeable resource;

    private ClosableResourceChannelListener(Closeable resource) {
      this.resource = resource;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (resource != null) {
        resource.close();
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.warn("Exception thrown while processing request", cause);
    ctx.close();
  }
}
