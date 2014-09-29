package tachyon.worker.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.stream.ChunkedWriteHandler;

import tachyon.worker.BlocksLocker;

/**
 * Adds the block server's pipeline into the channel.
 */
public final class PipelineHandler extends ChannelInitializer<SocketChannel> {
  private final BlocksLocker mLocker;

  public PipelineHandler(BlocksLocker locker) {
    mLocker = locker;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();
    pipeline.addLast("nioChunkedWriter", new ChunkedWriteHandler());
    pipeline.addLast("blockRequestDecoder", new BlockRequest.Decoder());
    pipeline.addLast("blockResponseEncoder", new BlockResponse.Encoder());
    pipeline.addLast("dataServerHandler", new DataServerHandler(mLocker));
  }
}
