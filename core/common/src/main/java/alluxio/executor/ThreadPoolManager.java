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

import alluxio.conf.Reconfigurable;
import alluxio.conf.ReconfigurableRegistry;
import alluxio.metrics.MetricsSystem;
import alluxio.util.CommonUtils;
import alluxio.wire.ThreadPoolExecutorInfo;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The threadPool manager which represents a manager to handle all thread pool executors.
 */
public class ThreadPoolManager {

  private static final Map<Object, ReconfigurableThreadPoolExecutor> THREAD_POOL_MAP =
      new HashMap();

  /**
   * Add a thread pool.
   * @param name the name of the thread pool
   * @param corePoolSizeSupplier the core pool size supplier
   * @param maximumPoolSizeSupplier the maximum pool size supplier
   * @param keepAliveTime the keep alive time
   * @param unit the unit
   * @param workQueue the work queue
   * @param threadFactory the thread factory
   * @return the registered thread pool
   */
  public static synchronized ThreadPoolExecutor newThreadPool(String name,
      Supplier<Integer> corePoolSizeSupplier,
      Supplier<Integer> maximumPoolSizeSupplier, long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    ThreadPoolExecutor threadPoolExecutor;
    threadPoolExecutor = new ThreadPoolExecutor(
        corePoolSizeSupplier.get(),
        maximumPoolSizeSupplier.get(),
        keepAliveTime, unit, workQueue, threadFactory);
    THREAD_POOL_MAP.put(threadPoolExecutor, new ReconfigurableThreadPoolExecutor(
        name, threadPoolExecutor, corePoolSizeSupplier, maximumPoolSizeSupplier));
    return threadPoolExecutor;
  }

  /**
   * Generates the thread pool executor info list.
   * @return the thread pool executor info list
   */
  public static synchronized List<ThreadPoolExecutorInfo> toThreadPoolExecutorInfo() {
    List<ThreadPoolExecutorInfo> threadPoolExecutorInfos = new ArrayList<>();
    for (ReconfigurableThreadPoolExecutor reconfigurableThreadPoolExecutor:
        THREAD_POOL_MAP.values()) {
      ThreadPoolExecutorInfo threadPoolExecutorInfo = new ThreadPoolExecutorInfo();
      ThreadPoolExecutor threadPoolExecutor = reconfigurableThreadPoolExecutor.mThreadPoolExecutor;
      threadPoolExecutorInfo.setName(reconfigurableThreadPoolExecutor.mName);
      threadPoolExecutorInfo.setMin(threadPoolExecutor.getCorePoolSize());
      threadPoolExecutorInfo.setMax(threadPoolExecutor.getMaximumPoolSize());
      threadPoolExecutorInfo.setActive(threadPoolExecutor.getActiveCount());
      threadPoolExecutorInfo.setCompleted(threadPoolExecutor.getCompletedTaskCount());
      threadPoolExecutorInfo.setCurrent(threadPoolExecutor.getPoolSize());
      threadPoolExecutorInfo.setQueueSize(threadPoolExecutor.getQueue().size());
      RejectedExecutionHandler rejectedHandler = threadPoolExecutor.getRejectedExecutionHandler();
      if (rejectedHandler instanceof MeasurableRejectedExecutionHandler) {
        threadPoolExecutorInfo.setRejected(
            ((MeasurableRejectedExecutionHandler) rejectedHandler).getCount());
      }
      threadPoolExecutorInfos.add(threadPoolExecutorInfo);
    }
    return threadPoolExecutorInfos;
  }

  /**
   * Unregister the thread pool executor related to the given key.
   *
   * @param key the key of thread pool executor to unregister
   */
  public static void unregister(Object key) {
    ReconfigurableThreadPoolExecutor reconfigurableThreadPoolExecutor =
        THREAD_POOL_MAP.get(key);
    if (reconfigurableThreadPoolExecutor != null) {
      reconfigurableThreadPoolExecutor.close();
    }
  }

  private static class ReconfigurableThreadPoolExecutor implements Reconfigurable, Closeable {
    private final String mName;
    private final ThreadPoolExecutor mThreadPoolExecutor;
    private final Supplier<Integer> mCorePoolSizeSupplier;
    private final Supplier<Integer> mMaximumPoolSizeSupplier;

    public ReconfigurableThreadPoolExecutor(String name, ThreadPoolExecutor threadPoolExecutor,
        Supplier<Integer> corePoolSizeSupplier,
        Supplier<Integer> maximumPoolSizeSupplier) {
      mName = name;
      mThreadPoolExecutor = threadPoolExecutor;
      mCorePoolSizeSupplier = corePoolSizeSupplier;
      mMaximumPoolSizeSupplier = maximumPoolSizeSupplier;

      MeasurableRejectedExecutionHandler measurableRejectedExecutionHandler =
          new MeasurableRejectedExecutionHandler(threadPoolExecutor.getRejectedExecutionHandler());
      threadPoolExecutor.setRejectedExecutionHandler(measurableRejectedExecutionHandler);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "ThreadActiveCount"),
          threadPoolExecutor::getActiveCount, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "ThreadCurrentCount"),
          threadPoolExecutor::getPoolSize, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "ThreadMaxCount"),
          threadPoolExecutor::getMaximumPoolSize, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "ThreadMinCount"),
          threadPoolExecutor::getCorePoolSize, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "CompleteTaskCount"),
          threadPoolExecutor::getCompletedTaskCount, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "ThreadQueueWaitingTaskCount"),
          threadPoolExecutor.getQueue()::size, 5, TimeUnit.SECONDS);
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          CommonUtils.PROCESS_TYPE + "." + name + "RejectCount"),
          measurableRejectedExecutionHandler::getCount, 5, TimeUnit.SECONDS);

      ReconfigurableRegistry.register(this);
    }

    @Override
    public void update() {
      int newCorePoolSize = mCorePoolSizeSupplier.get();
      int newMaximumPoolSize = mMaximumPoolSizeSupplier.get();
      if (newCorePoolSize != mThreadPoolExecutor.getPoolSize()
          && newMaximumPoolSize != mThreadPoolExecutor.getMaximumPoolSize()) {
        if (newCorePoolSize > mThreadPoolExecutor.getMaximumPoolSize()) {
          mThreadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
          mThreadPoolExecutor.setCorePoolSize(newCorePoolSize);
        } else {
          mThreadPoolExecutor.setCorePoolSize(newCorePoolSize);
          mThreadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
        }
        return;
      }
      if (newMaximumPoolSize != mThreadPoolExecutor.getMaximumPoolSize()) {
        mThreadPoolExecutor.setMaximumPoolSize(newMaximumPoolSize);
      } else if (newCorePoolSize != mThreadPoolExecutor.getCorePoolSize()) {
        mThreadPoolExecutor.setCorePoolSize(newCorePoolSize);
      }
    }

    @Override
    public void close() {
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "ThreadActiveCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "ThreadCurrentCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "ThreadMaxCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "ThreadMinCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "CompleteTaskCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "ThreadQueueWaitingTaskCount"));
      MetricsSystem.removeMetrics(MetricsSystem.getMetricName(CommonUtils.PROCESS_TYPE
          + "." + mName + "RejectCount"));
      ReconfigurableRegistry.unregister(this);
    }
  }
}
