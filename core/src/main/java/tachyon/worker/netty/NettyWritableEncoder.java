package tachyon.worker.netty;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

public final class NettyWritableEncoder extends MessageToMessageDecoder<NettyWritable> {
  @Override
  protected void decode(ChannelHandlerContext ctx, NettyWritable msg, List<Object> out)
      throws Exception {
    out.addAll(msg.write(ctx.alloc()));
  }
}
