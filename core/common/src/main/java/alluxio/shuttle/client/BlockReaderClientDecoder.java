package alluxio.shuttle.client;

import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class BlockReaderClientDecoder extends ByteToMessageDecoder {

  private Integer size;

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in,
                        List<Object> out) throws Exception {
    if (null == size && in.readableBytes() < 4) {
      return;
    }
    size = in.readInt();
    byte[] bytes = new byte[size];
    in.readBytes(bytes);
    String content = new String(bytes);
    out.add(content);
  }
}
