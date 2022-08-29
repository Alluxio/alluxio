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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Util class for ExecutorService.
 */
public class ExecutorServiceUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceUtils.class);

  /**
   * ShutdownAndAwaitTermination with default timeout.
   * @param executorService
   */
  public static void shutdownAndAwaitTermination(ExecutorService executorService) {
    shutdownAndAwaitTermination(executorService, 1000);
  }

  /**
   * Gracefully shutdown ExecutorService method.
   * @param executorService
   * @param timeoutMillis
   */
  public static void shutdownAndAwaitTermination(ExecutorService executorService,
                                                 long timeoutMillis) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(timeoutMillis, TimeUnit.MILLISECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
