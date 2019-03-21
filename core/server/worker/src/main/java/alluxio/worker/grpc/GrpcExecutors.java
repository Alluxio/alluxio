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

package alluxio.worker.grpc;

import alluxio.conf.ServerConfiguration;
import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.util.ThreadFactoryUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Executors for gRPC block server.
 */
@ThreadSafe
final class GrpcExecutors {
  private static final long THREAD_STOP_MS = Constants.SECOND_MS * 10;
  private static final int THREADS_MIN = 4;

  public static final ExecutorService ASYNC_CACHE_MANAGER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(512),
          ThreadFactoryUtils.build("AsyncCacheManagerExecutor-%d", true));

  public static final ExecutorService BLOCK_READER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_BLOCK_READER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("BlockDataReaderExecutor-%d", true));

  public static final ExecutorService BLOCK_WRITER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_BLOCK_WRITER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("BlockDataWriterExecutor-%d", true));

  /**
   * Private constructor.
   */
  private GrpcExecutors() {}
}
