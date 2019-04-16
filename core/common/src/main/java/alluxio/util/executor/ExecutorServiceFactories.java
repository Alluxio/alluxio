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

package alluxio.util.executor;

import alluxio.util.ThreadFactoryUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Convenience methods for constructing instances of {@link ExecutorServiceFactory}.
 */
public final class ExecutorServiceFactories {
  /**
   * Returns a {@link ExecutorServiceFactory} which creates threadpool executors with the given base
   * name. Created threads will be daemonic.
   *
   * @param name the base name for executor thread names
   * @return the {@link ExecutorServiceFactory}
   */
  public static ExecutorServiceFactory cachedThreadPool(String name) {
    return () -> Executors.newCachedThreadPool(ThreadFactoryUtils.build(name + "-%d", true));
  }

  /**
   * Returns a {@link ExecutorServiceFactory} which creates threadpool executors with the given base
   * name and number of threads. Created threads will be daemonic.
   *
   * @param name the base name for executor thread names
   * @param nThreads the number of threads to create executors with
   * @return the {@link ExecutorServiceFactory}
   */
  public static ExecutorServiceFactory fixedThreadPool(String name, int nThreads) {
    return () -> Executors.newFixedThreadPool(nThreads,
        ThreadFactoryUtils.build(name + "-%d", true));
  }

  /**
   * @param executorService the executor service to supply
   * @return an {@link ExecutorServiceFactory} which always returns the given
   *         {@link ExecutorService}
   */
  public static ExecutorServiceFactory constantExecutorServiceFactory(
      ExecutorService executorService) {
    return () -> executorService;
  }

  private ExecutorServiceFactories() {} // Not intended for instantiation.
}
