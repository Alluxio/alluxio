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

package alluxio.executor;

import alluxio.concurrent.jsr.ForkJoinPool;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.AlluxioExecutorService;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Used to create {@link ExecutorService} instances dynamically by configuration.
 */
public class ExecutorServiceBuilder {
  /**
   * Creates an {@link ExecutorService} for given Alluxio process dynamically by configuration.
   *
   * @param executorHost Where the executor is needed
   * @return instance of {@link ExecutorService}
   */
  public static AlluxioExecutorService buildExecutorService(RpcExecutorHost executorHost) {
    // Get executor type for given host.
    RpcExecutorType executorType = ServerConfiguration.getEnum(
        PropertyKey.Template.RPC_EXECUTOR_TYPE.format(executorHost.toString()),
        RpcExecutorType.class);
    // Build thread name format.
    String threadNameFormat =
        String.format("%s-rpc-executor-%s-thread", executorHost, executorType) + "-%d";
    // Read shared configuration for all supported executors.
    int corePoolSize = ServerConfiguration
        .getInt(PropertyKey.Template.RPC_EXECUTOR_CORE_POOL_SIZE.format(executorHost.toString()));
    int maxPoolSize = ServerConfiguration
        .getInt(PropertyKey.Template.RPC_EXECUTOR_MAX_POOL_SIZE.format(executorHost.toString()));
    long keepAliveMs = ServerConfiguration
        .getMs(PropertyKey.Template.RPC_EXECUTOR_KEEPALIVE.format(executorHost.toString()));
    // Property validation.
    Preconditions.checkArgument(keepAliveMs > 0L,
        String.format(
            "Cannot start Alluxio %s gRPC thread pool with %s=%s. "
                + "The keepalive time must be greater than 0!",
            executorHost,
            PropertyKey.Template.RPC_EXECUTOR_KEEPALIVE.format(executorHost.toString()),
            keepAliveMs));

    // Create the executor service.
    ExecutorService executorService = null;
    if (executorType == RpcExecutorType.FJP) {
      // Read FJP specific configurations.
      int parallelism = ServerConfiguration.getInt(
          PropertyKey.Template.RPC_EXECUTOR_FJP_PARALLELISM.format(executorHost.toString()));
      int minRunnable = ServerConfiguration.getInt(
          PropertyKey.Template.RPC_EXECUTOR_FJP_MIN_RUNNABLE.format(executorHost.toString()));
      boolean isAsync = ServerConfiguration
          .getBoolean(PropertyKey.Template.RPC_EXECUTOR_FJP_ASYNC.format(executorHost.toString()));
      // Property validation.
      Preconditions.checkArgument(parallelism > 0,
          String.format(
              "Cannot start Alluxio %s gRPC thread pool with %s=%s! "
                  + "The parallelism must be greater than 0!",
              executorHost,
              PropertyKey.Template.RPC_EXECUTOR_FJP_PARALLELISM.format(executorHost.toString()),
              parallelism));
      Preconditions.checkArgument(parallelism <= maxPoolSize,
          String.format(
              "Cannot start Alluxio %s gRPC thread pool with " + "%s=%s greater than %s=%s!",
              executorHost,
              PropertyKey.Template.RPC_EXECUTOR_FJP_PARALLELISM.format(executorHost.toString()),
              parallelism, PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE.toString(), maxPoolSize));
      // Create ForkJoinPool.
      executorService = new ForkJoinPool(parallelism,
          ThreadFactoryUtils.buildFjp(threadNameFormat, true), null, isAsync, corePoolSize,
          maxPoolSize, minRunnable, null, keepAliveMs, TimeUnit.MILLISECONDS);
    } else { // TPE
      // Read TPE specific configuration.
      boolean allowCoreThreadsTimeout = ServerConfiguration
          .getBoolean(PropertyKey.Template.RPC_EXECUTOR_TPE_ALLOW_CORE_THREADS_TIMEOUT
              .format(executorHost.toString()));
      // Read TPE queue type.
      ThreadPoolExecutorQueueType queueType = ServerConfiguration.getEnum(
          PropertyKey.Template.RPC_EXECUTOR_TPE_QUEUE_TYPE.format(executorHost.toString()),
          ThreadPoolExecutorQueueType.class);
      // Create internal queue.
      BlockingQueue<Runnable> queue = null;
      switch (queueType) {
        case LINKED_BLOCKING_QUEUE:
          queue = new LinkedBlockingQueue<>();
          break;
        case LINKED_BLOCKING_QUEUE_WITH_CAP:
          queue = new LinkedBlockingQueue<>(maxPoolSize);
          break;
        case ARRAY_BLOCKING_QUEUE:
          queue = new ArrayBlockingQueue<>(maxPoolSize);
          break;
        case SYNCHRONOUS_BLOCKING_QUEUE:
          queue = new SynchronousQueue<>();
          break;
        default:
          throw new IllegalArgumentException(
              String.format("Unsupported internal queue type: %s", queueType));
      }
      // Create ThreadPoolExecutor.
      executorService = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveMs,
          TimeUnit.MILLISECONDS, queue, ThreadFactoryUtils.build(threadNameFormat, true));
      // Post settings.
      ((ThreadPoolExecutor) executorService).allowCoreThreadTimeOut(allowCoreThreadsTimeout);
    }
    return new AlluxioExecutorService(executorService);
  }

  /**
   * Type of Alluxio process for generating RPC ExecutorServices.
   */
  public enum RpcExecutorHost {
    MASTER(0),
    JOB_MASTER(1),
    WORKER(2);

    private final int mValue;

    RpcExecutorHost(int value) {
      mValue = value;
    }

    @Override
    public String toString() {
      switch (mValue) {
        case 0:
          return "master";
        case 1:
          return "job.master";
        case 2:
          return "worker";
        default:
          return "<unrecognized_rpc_host>";
      }
    }
  }
}
