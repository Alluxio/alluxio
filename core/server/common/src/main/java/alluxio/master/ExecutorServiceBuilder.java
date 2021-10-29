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

package alluxio.master;

import alluxio.concurrent.jsr.ForkJoinPool;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
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
 * Type of Alluxio process for generating RPC ExecutorServices.
 */
enum RpcExecutorHost {
  MASTER(0), JOB_MASTER(1), WORKER(2);

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
        return null;
    }
  }
}

/**
 * RPC ExecutorService types.
 */
enum RpcExecutorType {
  ThreadPoolExecutor(0), // ThreadPoolExecutor
  ForkJoinPool(1); // ForkJoinPool

  private final int mValue;

  RpcExecutorType(int value) {
    mValue = value;
  }

  @Override
  public String toString() {
    switch (mValue) {
      case 0:
        return "tpe";
      case 1:
        return "fjp";
      default:
        return null;
    }
  }
}

/**
 * Internal task queue type for ThreadPoolExecutor:
 * -LINKED_BLOCKING_QUEUE
 * -LINKED_BLOCKING_QUEUE_WITH_CAP
 * -ARRAY_BLOCKING_QUEUE
 * -SYNCHRONOUS_BLOCKING_QUEUE
 */
enum ThreadPoolExecutorQueueType {
  LINKED_BLOCKING_QUEUE(0),
  LINKED_BLOCKING_QUEUE_WITH_CAP(1),
  ARRAY_BLOCKING_QUEUE(2),
  SYNCHRONOUS_BLOCKING_QUEUE(3);

  private final int mValue;

  ThreadPoolExecutorQueueType(int value) {
    mValue = value;
  }

  @Override
  public String toString() {
    switch (mValue) {
      case 0:
        return "LinkedBlockingQueue";
      case 1:
        return "LinkedBlockingQueue-WithCap";
      case 2:
        return "ArrayBlockingQueue";
      case 3:
        return "SynchronousBlockingQueue";
      default:
        return null;
    }
  }
}

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
    String threadNameFormat = executorHost + "-rpc-executor-thread-%d";
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
            executorHost, PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE, keepAliveMs));

    // Create the executor service.
    ExecutorService executorService = null;
    if (executorType == RpcExecutorType.ForkJoinPool) {
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
      executorService = new ForkJoinPool(
          parallelism,
          ThreadFactoryUtils.buildFjp(threadNameFormat, true), 
          null,
          isAsync, 
          corePoolSize,
          maxPoolSize, 
          minRunnable, 
          null, 
          keepAliveMs, 
          TimeUnit.MILLISECONDS);
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
          throw new IllegalArgumentException();
      }
      // Create ThreadPoolExecutor.
      executorService = new ThreadPoolExecutor(
          corePoolSize, 
          maxPoolSize, 
          keepAliveMs,
          TimeUnit.MILLISECONDS,
          queue, 
          ThreadFactoryUtils.build(threadNameFormat, true));
      // Post settings.
      ((ThreadPoolExecutor) executorService).allowCoreThreadTimeOut(allowCoreThreadsTimeout);
    }
    return new AlluxioExecutorService(executorService);
  }
}
