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

package alluxio.worker;

import alluxio.Constants;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This is the base class for all workers, and contains common functionality.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public abstract class AbstractWorker implements Worker {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWorker.class);
  private static final long SHUTDOWN_TIMEOUT_MS = 10L * Constants.SECOND_MS;

  /** A factory for creating executor services when they are needed. */
  private final ExecutorServiceFactory mExecutorServiceFactory;
  /** The executor service for the worker. */
  private ExecutorService mExecutorService;

  /**
   * @param executorServiceFactory executor service factory to use internally
   */
  protected AbstractWorker(ExecutorServiceFactory executorServiceFactory) {
    mExecutorServiceFactory =
        Preconditions.checkNotNull(executorServiceFactory, "executorServiceFactory");
  }

  /**
   * @return the executor service
   */
  protected ExecutorService getExecutorService() {
    return mExecutorService;
  }

  @Override
  public void start(WorkerNetAddress address) throws IOException {
    Preconditions.checkState(mExecutorService == null);
    mExecutorService = mExecutorServiceFactory.create();
  }

  @Override
  public void stop() throws IOException {
    // Shut down the executor service, interrupting any running threads.
    if (mExecutorService != null) {
      try {
        mExecutorService.shutdownNow();
        String awaitFailureMessage =
            "waiting for {} executor service to shut down. Daemons may still be running";
        try {
          if (!mExecutorService.awaitTermination(SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            LOG.warn("Timed out " + awaitFailureMessage, this.getClass().getSimpleName());
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while " + awaitFailureMessage, this.getClass().getSimpleName());
        }
      } finally {
        mExecutorService = null;
      }
    }
  }

  @Override
  public void close() throws IOException {
    stop();
  }
}
