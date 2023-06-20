/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.s3;

import static io.netty.handler.codec.http.HttpHeaderNames.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaderValues.KEEP_ALIVE;
import static org.eclipse.jetty.http.HttpHeaderValue.CLOSE;
import alluxio.client.file.FileSystem;
import alluxio.s3.S3ErrorResponse;
import alluxio.worker.dora.DoraWorker;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(S3HttpHandler.class);

  private final FileSystem mFileSystem;

  private final DoraWorker mDoraWorker;

  public S3HttpHandler(FileSystem fileSystem, DoraWorker doraWorker) {
    mFileSystem = fileSystem;
    mDoraWorker = doraWorker;
  }

  //  private final PagedService mPagedService;
  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request) throws Exception {
    S3NettyHandler s3Handler = null;
    try {
      boolean keepAlive = HttpUtil.isKeepAlive(request);
      s3Handler = S3NettyHandler.createHandler(context, request, mFileSystem, mDoraWorker);
      HttpResponse response = s3Handler.getS3Task().continueTask();

      if (keepAlive) {
        if (!request.protocolVersion().isKeepAliveDefault()) {
          response.headers().set(CONNECTION, KEEP_ALIVE);
        }
      } else {
        // Tell the client we're going to close the connection.
        response.headers().set(CONNECTION, CLOSE);
      }
      ChannelFuture f = context.writeAndFlush(response);
      if (!keepAlive) {
        f.addListener(ChannelFutureListener.CLOSE);
      }
    } catch (Exception ex) {
      HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, request.uri());
      ChannelFuture f = context.writeAndFlush(errorResponse);
      f.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
