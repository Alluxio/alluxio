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

package alluxio.master.file.mdsync;

import alluxio.master.file.meta.UfsSyncPathCache;
import alluxio.util.ThreadFactoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Takes {@link LoadResult} objects and processes them in an executor service.
 */
class LoadResultExecutor implements Closeable {

  private final ExecutorService mExecutor;
  private final UfsSyncPathCache mSyncPathCache;
  private final SyncProcess mSyncProcess;

  LoadResultExecutor(
      SyncProcess syncProcess,
      int executorThreads, UfsSyncPathCache syncPathCache) {
    mExecutor = Executors.newFixedThreadPool(executorThreads,
        ThreadFactoryUtils.build("mdsync-perform-sync-%d", true));
    mSyncPathCache = syncPathCache;
    mSyncProcess = syncProcess;
  }

  void processLoadResult(
      LoadResult result, Runnable beforeProcessing, Consumer<SyncProcessResult> onComplete,
      Consumer<Throwable> onError) {
    mExecutor.submit(() -> {
      beforeProcessing.run();
      try {
        onComplete.accept(
            mSyncProcess.performSync(result, mSyncPathCache)
        );
      } catch (DefaultSyncProcess.MountPointNotFoundRuntimeException e) {
        result.getTaskInfo().getStats().reportSyncFailReason(
            result.getLoadRequest(), result,
            SyncFailReason.PROCESSING_MOUNT_POINT_DOES_NOT_EXIST, e);
        onError.accept(e);
      } catch (Throwable t) {
        result.getTaskInfo().getStats().reportSyncFailReason(
            result.getLoadRequest(), result, SyncFailReason.PROCESSING_UNKNOWN, t);
        onError.accept(t);
      }
    });
  }

  @Override
  public void close() throws IOException {
    mExecutor.shutdown();
  }
}
