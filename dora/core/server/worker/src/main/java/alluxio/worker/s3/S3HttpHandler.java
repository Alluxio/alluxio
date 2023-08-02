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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Handles S3 HTTP request.
 */
public class S3HttpHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(S3HttpHandler.class);

  private final FileSystem mFileSystem;
  private final DoraWorker mDoraWorker;
  private final AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter;
  private final ThreadPoolExecutor mLightPool;
  private final ThreadPoolExecutor mHeavyPool;
  private S3NettyHandler mHandler;

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
  public void channelRead(ChannelHandlerContext context, Object msg)
      throws Exception {
    try {
      HttpResponse response = null;
      if (msg instanceof HttpRequest) {
        HttpRequest request = (HttpRequest) msg;
        try {
          mHandler =
              S3NettyHandler.createHandler(context, request, mFileSystem, mDoraWorker,
                  mAsyncAuditLogWriter);
        } catch (Exception ex) {
          LOG.error("Exception during create s3handler: ", ex);
          HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, request.uri());
          ChannelFuture f = context.writeAndFlush(errorResponse);
          f.addListener(ChannelFutureListener.CLOSE);
          return;
        }
        // Handle request async
        if (!mHandler.getS3Task().getOPType().equals(S3NettyBaseTask.OpType.PutObject)
            && Configuration.getBoolean(PropertyKey.WORKER_S3_ASYNC_PROCESS_ENABLED)) {
          S3NettyBaseTask.OpTag opTag = mHandler.getS3Task().mOPType.getOpTag();
          ExecutorService es =
              (ExecutorService) (opTag == S3NettyBaseTask.OpTag.LIGHT ? mLightPool : mHeavyPool);

          es.submit(() -> {
            try {
              HttpResponse asyncResponse = mHandler.getS3Task().continueTask();
              mHandler.processHttpResponse(asyncResponse);
            } catch (Throwable th) {
              HttpResponse errorResponse =
                  S3ErrorResponse.createNettyErrorResponse(th, request.uri());
              ChannelFuture f = context.writeAndFlush(errorResponse);
              f.addListener(ChannelFutureListener.CLOSE);
            }
          });
          return;
        }
        response = mHandler.getS3Task().continueTask();
      } else if (msg instanceof HttpContent && mHandler != null) {
        HttpContent content = (HttpContent) msg;
        response = mHandler.getS3Task().handleContent(content);
      } else {
        ByteBuf contentBuffer =
            Unpooled.copiedBuffer("Failed to encode HTTP request.", CharsetUtil.UTF_8);
        HttpResponse errorResponse =
            S3ErrorResponse.generateS3ErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                contentBuffer, HttpHeaderValues.TEXT_PLAIN);
        ChannelFuture f = context.writeAndFlush(errorResponse);
        f.addListener(ChannelFutureListener.CLOSE);
        return;
      }
      if (response != null && mHandler != null) {
        mHandler.processHttpResponse(response);
      }
    } catch (Exception ex) {
      HttpResponse errorResponse;
      if (msg instanceof HttpRequest) {
        errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, ((HttpRequest) msg).uri());
      } else if (mHandler != null) {
        errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, mHandler.getRequest().uri());
      } else {
        errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, "internal error");
      }
      ChannelFuture f = context.writeAndFlush(errorResponse);
      f.addListener(ChannelFutureListener.CLOSE);
    } finally {
      ReferenceCountUtil.release(msg);
    }
  }
}
