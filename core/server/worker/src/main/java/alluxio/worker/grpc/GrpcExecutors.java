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
import alluxio.security.User;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.UniqueBlockingQueue;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
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
public final class GrpcExecutors {
  private static final long THREAD_STOP_MS = Constants.SECOND_MS * 10;
  private static final int THREADS_MIN = 4;

  public static final ExecutorService CACHE_MANAGER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS,
          new UniqueBlockingQueue<>(ServerConfiguration.getInt(
              PropertyKey.WORKER_NETWORK_ASYNC_CACHE_MANAGER_QUEUE_MAX)),
          ThreadFactoryUtils.build("CacheManagerExecutor-%d", true)));

  public static final ExecutorService BLOCK_READER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_BLOCK_READER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("BlockDataReaderExecutor-%d", true)));

  public static final ExecutorService BLOCK_READER_SERIALIZED_RUNNER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_BLOCK_READER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(32),
          ThreadFactoryUtils.build("BlockDataReaderSerializedExecutor-%d", true),
          new ThreadPoolExecutor.CallerRunsPolicy()));

  public static final ExecutorService BLOCK_WRITER_EXECUTOR =
      new ImpersonateThreadPoolExecutor(new ThreadPoolExecutor(THREADS_MIN,
          ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_BLOCK_WRITER_THREADS_MAX),
          THREAD_STOP_MS, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
          ThreadFactoryUtils.build("BlockDataWriterExecutor-%d", true)));

  /**
   * Private constructor.
   */
  private GrpcExecutors() {}

  /**
   * This executor passes impersonation information to the real worker thread.
   * The proxy user is tracked by {@link AuthenticatedClientUser#sUserThreadLocal}.
   * This executor delegates operations to the underlying executor while setting the
   * ThreadLocal context for execution.
   * */
  private static class ImpersonateThreadPoolExecutor extends AbstractExecutorService {
    private final ExecutorService mDelegate;

    public ImpersonateThreadPoolExecutor(ExecutorService service) {
      mDelegate = service;
    }

    @Override
    public void execute(final Runnable command) {
      // If there's no impersonation, proxyUser is just null
      User proxyUser = AuthenticatedClientUser.getOrNull();
      mDelegate.execute(() -> {
        try {
          AuthenticatedClientUser.set(proxyUser);
          command.run();
        } finally {
          AuthenticatedClientUser.remove();
        }
      });
    }

    @Override
    public void shutdown() {
      mDelegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return mDelegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return mDelegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return mDelegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return mDelegate.awaitTermination(timeout, unit);
    }
  }
}
