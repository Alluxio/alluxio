package alluxio.worker.http;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {

    //private final SslContext sslCtx;

    public HttpServerInitializer() {
        //this.sslCtx = sslCtx;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        //if (sslCtx != null) {
        //    p.addLast(sslCtx.newHandler(ch.alloc()));
        //}
        p.addLast(new HttpServerCodec());
        p.addLast(new HttpContentCompressor((CompressionOptions[]) null));
        p.addLast(new HttpServerExpectContinueHandler());
        p.addLast(new HttpServerHandler());
    }
}