package alluxio.shuttle.client;

import alluxio.grpc.ReadRequest;
import alluxio.shuttle.server.BlockReaderServerHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class BlockReaderClientHandler extends ChannelInboundHandlerAdapter {

  private final ReadRequest readRequest;

  public BlockReaderClientHandler(ReadRequest readRequest) {
    this.readRequest = readRequest;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    String m = (String) msg;
    System.out.println(m);
    ctx.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }
}