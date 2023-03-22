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

package alluxio.master.mdsync;

import alluxio.util.ThreadFactoryUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Takes {@link LoadResult} objects and processes them in an executor service.
 */
class LoadResultExecutor {

  private final ExecutorService mExecutor;

  LoadResultExecutor(int executorThreads) {
    mExecutor = Executors.newFixedThreadPool(executorThreads,
        ThreadFactoryUtils.build("mdsync-perform-sync", true));
  }

  void processLoadResult(
      LoadResult result, Runnable beforeProcessing, Consumer<SyncProcessResult> onComplete,
      Consumer<Throwable> onError) {
    mExecutor.submit(() -> {
      beforeProcessing.run();
      try {
        onComplete.accept(SyncProcess.performSync(result));
      } catch (Throwable t) {
        onError.accept(t);
      }
    });
  }
}
