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

package alluxio.client.file;

import alluxio.concurrent.CountingLatch;
import alluxio.conf.PropertyKey;
import alluxio.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Reinitializes {@link FileSystemContext} inside {@link BaseFileSystem}.
 *
 * A daemon heartbeat thread periodically fetches configuration hashes from meta master,
 * if they differ from the hashes in the {@link alluxio.ClientContext} backing the
 * {@link FileSystemContext}, it tries to reinitialize the {@link FileSystemContext}.
 *
 * Each RPC needs to call {@link #block()} to mark its lifetime, when there
 * are ongoing RPCs executing between these two methods, reinitialization is blocked.
 *
 * Reinitialization starts when there are no ongoing RPCs, after starting, all further RPCs
 * are blocked until the reinitialization finishes or until timeout. If it succeeds, future RPCs
 * will use the reinitialized context, otherwise, an exception is thrown from
 * {@link #block()}.
 */
public final class FileSystemContextReinitializer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemContextReinitializer.class);

  private final FileSystemContext mContext;
  private final ConfigHashSync mExecutor;
  private Future mFuture;
  private CountingLatch mLatch = new CountingLatch();

  private static final int REINIT_EXECUTOR_THREADPOOL_SIZE = 1;
  private static final ScheduledExecutorService REINIT_EXECUTOR =
      new ScheduledThreadPoolExecutor(REINIT_EXECUTOR_THREADPOOL_SIZE,
          ThreadFactoryUtils.build("config-hash-master-heartbeat-%d", true));

  /**
   * Creates a new reinitializer for the context.
   *
   * The heartbeat will be started.
   *
   * @param context the context to be reinitialized
   */
  public FileSystemContextReinitializer(FileSystemContext context) {
    mContext = context;
    mExecutor = new ConfigHashSync(context);
    mFuture = REINIT_EXECUTOR.scheduleAtFixedRate(() -> {
      try {
        mExecutor.heartbeat();
      } catch (Exception e) {
        LOG.error("Uncaught exception in config hearbeat executor, shutting down", e);
      }
    }, 0, mContext.getClientContext().getClusterConf().getMs(PropertyKey.USER_CONF_SYNC_INTERVAL),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Notifies that the reinitialization has succeeded.
   *
   * It will reset internal state that might be affected by the reinitialized context,
   * e.g. the meta master client used in the heartbeat might need to be reset because of
   * new configurations.
   */
  public void onSuccess() {
    mExecutor.resetMetaMasterConfigClient(mContext.getMasterClientContext());
  }

  /**
   * This resource blocks reinitialization until close.
   */
  public static final class ReinitBlockerResource implements Closeable {
    private CountingLatch mLatch;

    /**
     * Increases the count of the latch.
     * If the latch is held by {@link ReinitAllowerResource}, the constructor blocks until the latch
     * is released by {@link ReinitAllowerResource#close()}.
     *
     * @param latch the count latch
     * @throws InterruptedException if interrupted during being blocked
     */
    public ReinitBlockerResource(CountingLatch latch) throws InterruptedException {
      mLatch = latch;
      mLatch.inc();
    }

    @Override
    public void close() {
      mLatch.dec();
    }
  }

  /**
   * This resource allows reinitialization.
   * It blocks further RPCs until close.
   */
  public static final class ReinitAllowerResource implements Closeable {
    private CountingLatch mLatch;

    /**
     * Waits the count to reach zero, then blocks further constructions of
     * {@link ReinitBlockerResource} until {@link #close()}.
     *
     * @param latch the count latch
     */
    public ReinitAllowerResource(CountingLatch latch) {
      mLatch = latch;
      mLatch.await();
    }

    @Override
    public void close() {
      mLatch.release();
    }
  }

  /**
   * Acquires the resource to block reinitialization.
   *
   * When the context is being reinitialized, this call blocks.
   * If the reinitialization fails, an exception is thrown.
   *
   * If there is an existing reinitialization exception, immediately throw an exception
   * without trying to block further reinitialization.
   *
   * @throws IOException when reinitialization fails before or during this method
   * @throws InterruptedException if interrupted during being blocked
   * @return the resource
   */
  public ReinitBlockerResource block() throws IOException, InterruptedException {
    Optional<IOException> exception = mExecutor.getException();
    if (exception.isPresent()) {
      throw exception.get();
    }
    ReinitBlockerResource r = new ReinitBlockerResource(mLatch);
    // Check exception again in case the reinit fails when inc is blocked.
    exception = mExecutor.getException();
    if (exception.isPresent()) {
      r.close();
      throw exception.get();
    }
    return r;
  }

  /**
   * Acquires the resource to allow reinitialization.
   *
   * This call blocks until there are no ongoing operations holding the resource returned by
   * {@link #block()}.
   * When it returns, further calls to {@link #block()} block until the
   * returned resource is closed.
   *
   * @return the resource
   */
  public ReinitAllowerResource allow() {
    return new ReinitAllowerResource(mLatch);
  }

  /**
   * Shuts down the heartbeat thread immediately.
   *
   * If already closed, this is a noop.
   */
  public void close() {
    if (mFuture != null) {
      mFuture.cancel(true);
      mFuture = null;

      mExecutor.close();
    }
  }
}
