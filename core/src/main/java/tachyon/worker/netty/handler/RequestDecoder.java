package tachyon.worker.netty.handler;

import java.util.List;

import com.google.common.base.Optional;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import tachyon.worker.BlocksLocker;
import tachyon.worker.netty.NettyWritableEncoder;
import tachyon.worker.netty.protocol.GetBlock;
import tachyon.worker.netty.protocol.RequestHeader;
import tachyon.worker.netty.protocol.RequestType;

public final class RequestDecoder extends ByteToMessageDecoder {
  private final BlocksLocker mLocker;

  public RequestDecoder(BlocksLocker mLocker) {
    this.mLocker = mLocker;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    if (in.readableBytes() >= RequestHeader.HEADER_SIZE) {
      long version = in.readLong();
      Optional<RequestType> typeOpt = RequestType.valueOf(in.readInt());
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
        ctx.pipeline().remove(this);
      } else {
        // bad header, return a exception and close
      }
    }
  }

  private ChannelHandler[] getBlockPipeline() {
    return new ChannelHandler[] {
        new GetBlock.GetBlockDecoder(),
        new GetBlockHandler(mLocker),
        new NettyWritableEncoder()
    };
  }

  private ChannelHandler[] putBlockPipeline() {
    return new ChannelHandler[] {

    };
  }
}
