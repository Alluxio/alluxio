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
import alluxio.worker.dora.DoraWorker;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.compression.CompressionOptions;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpServerExpectContinueHandler;

/**
 * Adds the http server's pipeline into the channel.
 */
public class S3HttpPipelineHandler extends ChannelInitializer<SocketChannel> {
  private final FileSystem mFileSystem;
  private final DoraWorker mDoraWorker;
  private AsyncUserAccessAuditLogWriter mAsyncAuditLogWriter = null;

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
  }

  @Override
  protected void initChannel(SocketChannel channel) throws Exception {
    ChannelPipeline pipeline = channel.pipeline();
    pipeline.addLast(new HttpServerCodec());
    pipeline.addLast(new HttpContentCompressor((CompressionOptions[]) null));
    pipeline.addLast(new HttpObjectAggregator(512 * 1024));
    pipeline.addLast(new HttpServerExpectContinueHandler());
    pipeline.addLast(new S3HttpHandler(mFileSystem, mDoraWorker, mAsyncAuditLogWriter));
  }
}
