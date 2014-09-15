package tachyon.worker.netty.handler;

import com.google.common.base.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.stream.ChunkedWriteHandler;

import io.netty.util.ReferenceCountUtil;
import tachyon.worker.BlocksLocker;
import tachyon.worker.netty.NettyWritableEncoder;
import tachyon.worker.netty.protocol.GetBlock;
import tachyon.worker.netty.protocol.RequestHeader;
import tachyon.worker.netty.protocol.RequestType;

public final class RequestDecoder extends ChannelInboundHandlerAdapter {
  private final BlocksLocker mLocker;

  public RequestDecoder(BlocksLocker mLocker) {
    this.mLocker = mLocker;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf data = (ByteBuf) msg;

      if (data.readableBytes() >= RequestHeader.HEADER_SIZE) {
        long version = data.readLong();
        Optional<RequestType> typeOpt = RequestType.valueOf(data.readInt());
        if (typeOpt.isPresent() && RequestHeader.CURRENT_VERSION == version) {
          // we can accept the header
          switch (typeOpt.get()) {
            case GetBlock:
              ctx.pipeline().addLast(getBlockPipeline());
              break;
            case PutBlock:
              ctx.pipeline().addLast(putBlockPipeline());
              break;
            default:
              // unsupported type!
              throw new AssertionError("Unknown type: " + typeOpt.get());
          }
          ctx.fireChannelRead(ReferenceCountUtil.retain(data));
          ctx.pipeline().remove(this);
        } else {
          // bad header, return a exception and close
        }
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

  private ChannelHandler[] getBlockPipeline() {
    return new ChannelHandler[] {
        // write path
        new ChunkedWriteHandler(),
        new NettyWritableEncoder(),

        // read path
        new GetBlock.GetBlockDecoder(),
        new GetBlockHandler(mLocker)
    };
  }

  private ChannelHandler[] putBlockPipeline() {
    return new ChannelHandler[] {

    };
  }
}
