package alluxio.shuttle.server;

import java.io.BufferedReader;
import java.io.FileReader;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class BlockReaderServerHandler extends ChannelInboundHandlerAdapter {

  public byte[] readBlock(int blockId) throws Exception {
    String resourcePath = this.getClass().getClassLoader().getResource(blockId + ".txt").getPath();
    BufferedReader bufferedReader = new BufferedReader(new FileReader(resourcePath));
    StringBuffer stringBuffer = new StringBuffer();
    String line = null;
    while ((line = bufferedReader.readLine()) != null) {
      stringBuffer.append(line + "\n");
    }
    byte[] bytes = stringBuffer.toString().getBytes();
    return bytes;
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception { // (1)
    byte[] bytes = readBlock(10080);
    final ByteBuf byteBuf = ctx.alloc().buffer(4 + bytes.length); // (2)
    byteBuf.writeInt(bytes.length);
    byteBuf.writeBytes(bytes);

    final ChannelFuture f = ctx.writeAndFlush(byteBuf); // (3)
    f.addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        assert f == future;
        ctx.close();
      }
    }); // (4)
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}