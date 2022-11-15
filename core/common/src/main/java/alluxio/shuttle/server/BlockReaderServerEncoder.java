package alluxio.shuttle.server;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.example.time.pojo.UnixTime;

public class BlockReaderServerEncoder extends ChannelOutboundHandlerAdapter {
  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    UnixTime m = (UnixTime) msg;
    ByteBuf encoded = ctx.alloc().buffer(4);
    encoded.writeInt((int)m.value());
    ctx.write(encoded, promise); // (1)
  }
}