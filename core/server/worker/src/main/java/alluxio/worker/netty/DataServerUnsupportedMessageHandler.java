package alluxio.worker.netty;

import alluxio.network.protocol.RPCErrorResponse;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.RPCResponse;
import alluxio.proto.dataserver.Protocol;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Netty handler which replies to the client with an unknown message error for message types
 * which are no longer supported.
 */
public class DataServerUnsupportedMessageHandler extends ChannelInboundHandlerAdapter {
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (object instanceof RPCProtoMessage) { // Unknown proto message, reply proto.
      RPCProtoMessage resp =
          RPCProtoMessage.createResponse(Protocol.Status.Code.UNIMPLEMENTED, "", null, null);
      ctx.writeAndFlush(resp);
    } else if (object instanceof RPCMessage){ // Unknown non-proto message, reply non-proto.
      RPCErrorResponse resp = new RPCErrorResponse(RPCResponse.Status.UNKNOWN_MESSAGE_ERROR);
      ctx.writeAndFlush(resp);
    } else { // Unknown message, this should not happen.
      ctx.fireChannelRead(object);
    }
  }
}
