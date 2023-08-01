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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Handles S3 HTTP request.
 */
public class S3HttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(S3HttpHandler.class);

  private final FileSystem mFileSystem;
  private final DoraWorker mDoraWorker;
  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private final ThreadPoolExecutor mLightPool;
  private final ThreadPoolExecutor mHeavyPool;

  /**
   * Constructs an instance of {@link S3HttpHandler}.
   *
   * @param fileSystem
   * @param doraWorker
   * @param asyncAuditLogWriter
   * @param lightPool
   * @param heavyPool
   */
  public S3HttpHandler(FileSystem fileSystem, DoraWorker doraWorker,
                       AsyncUserAccessAuditLogWriter asyncAuditLogWriter,
                       ThreadPoolExecutor lightPool, ThreadPoolExecutor heavyPool) {
    mFileSystem = fileSystem;
    mDoraWorker = doraWorker;
    mAsyncAuditLogWriter = asyncAuditLogWriter;
    mLightPool = lightPool;
    mHeavyPool = heavyPool;
  }

  //  private final PagedService mPagedService;
  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.flush();
  }

  @Override
  protected void channelRead0(ChannelHandlerContext context, FullHttpRequest request)
      throws Exception {
    S3NettyHandler s3Handler;
    try {
      s3Handler =
          S3NettyHandler.createHandler(context, request, mFileSystem, mDoraWorker,
              mAsyncAuditLogWriter);
    } catch (Exception ex) {
      LOG.error("Exception during create s3handler: ", ex);
      HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, request.uri());
      ChannelFuture f = context.writeAndFlush(errorResponse);
      f.addListener(ChannelFutureListener.CLOSE);
      return;
    }

    try {
      // Handle request async
      if (Configuration.getBoolean(PropertyKey.WORKER_S3_ASYNC_PROCESS_ENABLED)) {
        FullHttpRequest asyncRequest = request.copy();
        S3NettyBaseTask.OpTag opTag = s3Handler.getS3Task().mOPType.getOpTag();
        ExecutorService es = (ExecutorService) (opTag == S3NettyBaseTask.OpTag.LIGHT
            ? mLightPool : mHeavyPool);

        es.submit(() -> {
          try {
            HttpResponse response = s3Handler.getS3Task().continueTask();
            s3Handler.processHttpResponse(response);
          } catch (Throwable th) {
            HttpResponse errorResponse =
                S3ErrorResponse.createNettyErrorResponse(th, asyncRequest.uri());
            ChannelFuture f = context.writeAndFlush(errorResponse);
            f.addListener(ChannelFutureListener.CLOSE);
          }
        });
      } else {
        HttpResponse response = s3Handler.getS3Task().continueTask();
        s3Handler.processHttpResponse(response);
      }
    } catch (Exception ex) {
      HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, request.uri());
      s3Handler.processHttpResponse(errorResponse);
    }
  }
}
