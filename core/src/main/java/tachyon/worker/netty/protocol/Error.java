package tachyon.worker.netty.protocol;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;

import tachyon.worker.netty.NettyWritable;

public abstract class Error implements NettyWritable {
  private final ResponseType mType;

  public Error(ResponseType mType) {
    this.mType = mType;
  }

  @Override
  public List<Object> write(ByteBufAllocator allocator) throws IOException {
    return ImmutableList.<Object>of(allocator.buffer(Ints.BYTES).writeInt(mType.ordinal()));
  }

  public static ChannelFuture writeAndClose(Error error, ChannelHandlerContext ctx) {
    ChannelFuture future = ctx.writeAndFlush(error);
    return future.addListener(ChannelFutureListener.CLOSE);
  }
}
