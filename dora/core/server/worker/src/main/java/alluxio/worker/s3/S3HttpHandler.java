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

import alluxio.client.file.FileSystem;
import alluxio.master.audit.AsyncUserAccessAuditLogWriter;
import alluxio.s3.S3ErrorResponse;
import alluxio.worker.dora.DoraWorker;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles S3 HTTP request.
 */
public class S3HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(S3HttpHandler.class);

  private final FileSystem mFileSystem;

  private final DoraWorker mDoraWorker;

  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;

  /**
   * Constructs an instance of {@link S3HttpHandler}.
   * @param fileSystem
   * @param doraWorker
   * @param asyncAuditLogWriter
   */
  public S3HttpHandler(FileSystem fileSystem, DoraWorker doraWorker,
                       AsyncUserAccessAuditLogWriter asyncAuditLogWriter) {
    mFileSystem = fileSystem;
    mDoraWorker = doraWorker;
    mAsyncAuditLogWriter = asyncAuditLogWriter;
  }

  //  private final PagedService mPagedService;
  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request)
      throws Exception {
    try {
      S3NettyHandler s3Handler =
          S3NettyHandler.createHandler(context, request, mFileSystem, mDoraWorker,
              mAsyncAuditLogWriter);
      s3Handler.getS3Task().continueTask();
    } catch (Exception ex) {
      HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, request.uri());
      ChannelFuture f = context.writeAndFlush(errorResponse);
      f.addListener(ChannelFutureListener.CLOSE);
    }
  }
}
