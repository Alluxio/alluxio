package alluxio.worker.netty;

import alluxio.network.protocol.RPCProtoMessage;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Handler for async cache requests.
 */
public class AsyncCacheHandler extends ChannelInboundHandlerAdapter {
  /**
   * Constructs a new async cache handler.
   */
  public AsyncCacheHandler() {}

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (object instanceof RPCProtoMessage
        && ((RPCProtoMessage) object).getMessage().isAsyncCacheRequest()) {
      ctx.writeAndFlush(RPCProtoMessage.createOkResponse(null));
    } else {
      ctx.fireChannelRead(object);
    }
  }
}
