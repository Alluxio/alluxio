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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import javax.annotation.Nullable;

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
  private boolean mAsyncHandle;

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
    mAsyncHandle = false;
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
        mHandler = S3NettyHandler.createHandler(context, request, mFileSystem, mDoraWorker,
            mAsyncAuditLogWriter);

        // Handle request async
        if (Configuration.getBoolean(PropertyKey.WORKER_S3_ASYNC_PROCESS_ENABLED)) {
          S3NettyBaseTask.OpTag opTag = mHandler.getS3Task().mOPType.getOpTag();
          ExecutorService es =
              (ExecutorService) (opTag == S3NettyBaseTask.OpTag.LIGHT ? mLightPool : mHeavyPool);
          es.submit(new AsyncS3NettyTask(mHandler));
          mAsyncHandle = true;
          return;
        } else {
          response = mHandler.getS3Task().continueTask();
        }
      } else if (msg instanceof HttpContent) {
        HttpContent content = (HttpContent) msg;
        // if the req handle async, we skip the Empty content.
        if (mHandler.getS3Task().needContent()) {
          if (!mAsyncHandle) {
            response = mHandler.getS3Task().handleContent(content);
          } else {
            mHandler.addContent(content);
          }
        }
      } else {
        HttpResponse errorResponse =
            S3ErrorResponse.generateS3ErrorResponse(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Failed to encode HTTP request.", HttpHeaderValues.TEXT_PLAIN);
        context.writeAndFlush(errorResponse).addListener(ChannelFutureListener.CLOSE);
        return;
      }
      if (response != null && mHandler != null) {
        mHandler.processHttpResponse(response);
      }
    } catch (Throwable ex) {
      String resource = (mHandler != null) ? mHandler.getRequest().uri() : "internal error";
      HttpResponse errorResponse = S3ErrorResponse.createNettyErrorResponse(ex, resource);
      context.writeAndFlush(errorResponse).addListener(ChannelFutureListener.CLOSE);
    } finally {
      if (!mAsyncHandle) {
        ReferenceCountUtil.release(msg);
      }
    }
  }

  private static class AsyncS3NettyTask implements Runnable {
    private final S3NettyHandler mHandler;

    public AsyncS3NettyTask(S3NettyHandler handler) {
      mHandler = handler;
    }

    @Override
    public void run() {
      try {
        HttpResponse asyncResponse = mHandler.getS3Task().continueTask();
        // if the s3 task doesn't need to process content, just return the response.
        if (asyncResponse == null && mHandler.getS3Task().needContent()) {
          asyncResponse = processContent();
        }
        mHandler.processHttpResponse(asyncResponse);
      } catch (Throwable th) {
        HttpResponse errorResponse =
            S3ErrorResponse.createNettyErrorResponse(th, mHandler.getRequest().uri());
        mHandler.processHttpResponse(errorResponse);
      } finally {
        ReferenceCountUtil.release(mHandler.getRequest());
        while (mHandler.remainContent()) {
          try {
            HttpContent content = mHandler.getLatestContent();
            ReferenceCountUtil.release(content);
          } catch (InterruptedException e) {
            LOG.warn("skip uncaught content.");
          }
        }
      }
    }

    @Nullable
    private HttpResponse processContent() throws InterruptedException {
      HttpContent content;
      HttpResponse asyncResponse = null;
      while ((content = mHandler.getLatestContent()) != null) {
        try {
          asyncResponse = mHandler.getS3Task().handleContent(content);
          if (asyncResponse != null) {
            break;
          }
        } finally {
          ReferenceCountUtil.release(content);
        }
      }
      return asyncResponse;
    }
  }
}
