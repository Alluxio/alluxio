package alluxio.shuttle.client;

import alluxio.grpc.ReadRequest;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

public class BlockReaderRequestHandler extends ChannelInboundHandlerAdapter {

  private final ReadRequest mReadRequest;

  private final BlockingQueue<ByteBuf> blockingQueue = new LinkedBlockingQueue<>();

  public BlockReaderRequestHandler(ReadRequest readRequest) {
    this.mReadRequest = readRequest;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    ByteBuf byteBuf = ctx.alloc().buffer();
    byteBuf.writeBytes(mReadRequest.toByteArray());
    ctx.writeAndFlush(byteBuf);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
    blockingQueue.put((ByteBuf) msg);
    ctx.close();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
  }

  public ByteBuf getBlockData() throws InterruptedException {
    return blockingQueue.take();
  }

}