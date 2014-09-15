package tachyon.worker.netty;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.MessageToMessageEncoder;

public final class NettyWritableEncoder extends MessageToMessageEncoder<NettyWritable> {

  @Override
  protected void encode(ChannelHandlerContext ctx, NettyWritable msg, List<Object> out) throws Exception {
    out.addAll(msg.write(ctx.alloc()));
  }
}