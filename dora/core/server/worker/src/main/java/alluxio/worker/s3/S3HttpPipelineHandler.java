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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.dora.DoraWorker;

import com.google.common.base.Preconditions;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Adds the http server's pipeline into the channel.
 */
public class S3HttpPipelineHandler extends ChannelInitializer<SocketChannel> {
  private final FileSystem mFileSystem;
  private final DoraWorker mDoraWorker;
  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter = null;
  private ThreadPoolExecutor mLightPool;
  private ThreadPoolExecutor mHeavyPool;

  /**
   * Constructs an instance of {@link S3HttpPipelineHandler}.
   * @param fileSystem AlluxioFileSystem
   * @param doraWorker dora worker
   */
  public S3HttpPipelineHandler(FileSystem fileSystem, DoraWorker doraWorker) {
    mFileSystem = fileSystem;
    mDoraWorker = doraWorker;
    if (Configuration.getBoolean(PropertyKey.WORKER_S3_AUDIT_LOGGING_ENABLED)) {
      mAsyncAuditLogWriter = new AsyncUserAccessAuditLogWriter("NETTY_S3_AUDIT_LOG");
      mAsyncAuditLogWriter.start();
      MetricsSystem.registerGaugeIfAbsent(
          MetricKey.PROXY_AUDIT_LOG_ENTRIES_SIZE.getName(),
          () -> mAsyncAuditLogWriter != null
              ? mAsyncAuditLogWriter.getAuditLogEntriesSize() : -1);
    }

    if (Configuration.getBoolean(PropertyKey.WORKER_S3_ASYNC_PROCESS_ENABLED)) {
      mLightPool = createLightThreadPool();
      mHeavyPool = createHeavyThreadPool();
    }
  }

  @Override
  protected void initChannel(SocketChannel channel) throws Exception {
    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpObjectAggregator(512 * 1024));
    pipeline.addLast(new HttpServerExpectContinueHandler());
    pipeline.addLast(
        new S3HttpHandler(mFileSystem, mDoraWorker, mAsyncAuditLogWriter, mLightPool, mHeavyPool));
  }

  private ThreadPoolExecutor createLightThreadPool() {
    int lightCorePoolSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_CORE_THREAD_NUMBER);
    Preconditions.checkArgument(lightCorePoolSize > 0,
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_CORE_THREAD_NUMBER.getName()
            + " must be a positive integer.");
    int lightMaximumPoolSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_MAXIMUM_THREAD_NUMBER);
    Preconditions.checkArgument(lightMaximumPoolSize >= lightCorePoolSize,
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_MAXIMUM_THREAD_NUMBER.getName()
            + " must be greater than or equal to the value of "
            + PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_CORE_THREAD_NUMBER.getName());
    int lightPoolQueueSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_QUEUE_SIZE);
    Preconditions.checkArgument(lightPoolQueueSize > 0,
        PropertyKey.WORKER_S3_ASYNC_LIGHT_POOL_QUEUE_SIZE.getName()
            + " must be a positive integer.");
    return new ThreadPoolExecutor(lightCorePoolSize, lightMaximumPoolSize, 0,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(lightPoolQueueSize),
        ThreadFactoryUtils.build("S3-LIGHTPOOL-%d", false));
  }

  private ThreadPoolExecutor createHeavyThreadPool() {
    int heavyCorePoolSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_CORE_THREAD_NUMBER);
    Preconditions.checkArgument(heavyCorePoolSize > 0,
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_CORE_THREAD_NUMBER.getName()
            + " must be a positive integer.");
    int heavyMaximumPoolSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_MAXIMUM_THREAD_NUMBER);
    Preconditions.checkArgument(heavyMaximumPoolSize >= heavyCorePoolSize,
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_MAXIMUM_THREAD_NUMBER.getName()
            + " must be greater than or equal to the value of "
            + PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_CORE_THREAD_NUMBER.getName());
    int heavyPoolQueueSize = Configuration.getInt(
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_QUEUE_SIZE);
    Preconditions.checkArgument(heavyPoolQueueSize > 0,
        PropertyKey.WORKER_S3_ASYNC_HEAVY_POOL_QUEUE_SIZE.getName()
            + " must be a positive integer.");
    return new ThreadPoolExecutor(heavyCorePoolSize, heavyMaximumPoolSize, 0,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(heavyPoolQueueSize),
        ThreadFactoryUtils.build("S3-HEAVYPOOL-%d", false));
  }
}
