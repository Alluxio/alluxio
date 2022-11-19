package alluxio.shuttle.client;

import alluxio.grpc.ReadRequest;
import alluxio.wire.WorkerNetAddress;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;

public class NettyBlockReaderClient {

  private static final String DEFAULT_HOST = "127.0.0.1";
  private static final int DEFAULT_PORT = 8080;

  private final String serverHost;

  private final int serverPort;

  public NettyBlockReaderClient(String serverHost, int serverPort) {
    this.serverHost = serverHost;
    this.serverPort = serverPort;
  }

  public ByteBuf readBlock(ReadRequest readRequest) throws Exception {
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    BlockReaderRequestHandler blockReaderRequestHandler = new BlockReaderRequestHandler(readRequest);
    try {
      Bootstrap b = new Bootstrap(); // (1)
      b.group(workerGroup); // (2)
      b.channel(NioSocketChannel.class); // (3)
      b.option(ChannelOption.SO_KEEPALIVE, true); // (4)
      b.handler(new ChannelInitializer() {
        @Override
        protected void initChannel(Channel channel) throws Exception {
          channel.pipeline().addLast(blockReaderRequestHandler);
        }
      });

      // Start the client.
      ChannelFuture f = b.connect(serverHost, serverPort).sync(); // (5)
      // Wait until the connection is closed.
      f.channel().closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
    }
    return blockReaderRequestHandler.getBlockData();
  }

  public static void main(String[] args) throws Exception {
    String host = DEFAULT_HOST;
    int port = DEFAULT_PORT;
    if (args.length > 0) {
      host = args[0];
      port = Integer.parseInt(args[1]);
    }
    NettyBlockReaderClient blockReaderClient = new NettyBlockReaderClient(host, port);
    //blockReaderClient.readBlock();
  }
}