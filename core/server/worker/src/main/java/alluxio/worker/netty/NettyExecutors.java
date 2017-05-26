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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.util.ThreadFactoryUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Adds the block server's pipeline into the channel.
 */
@ThreadSafe
final class NettyExecutors {
  private static final long THREAD_STOP_MS = Constants.SECOND_MS * 10;
  private static final int THREADS_MIN = 4;

  public static final ExecutorService BLOCK_READER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BLOCK_READER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
          ThreadFactoryUtils.build("BlockPacketReaderExecutor-%d", true));

  public static final ExecutorService BLOCK_WRITER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BLOCK_WRITER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
          ThreadFactoryUtils.build("BlockPacketWriterExecutor-%d", true));

  public static final ExecutorService FILE_WRITER_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_FILE_WRITER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
          ThreadFactoryUtils.build("FilePacketWriterExecutor-%d", true));

  public static final ExecutorService RPC_EXECUTOR =
      new ThreadPoolExecutor(THREADS_MIN,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_RPC_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(),
          ThreadFactoryUtils.build("BlockOpenExecutor-%d", true));

  /**
   * Private constructor.
   */
  private NettyExecutors() {}
}
