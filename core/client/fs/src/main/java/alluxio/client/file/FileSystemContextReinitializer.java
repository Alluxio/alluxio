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

import alluxio.conf.PropertyKey;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.util.ThreadFactoryUtils;

import org.apache.commons.lang.time.DurationFormatUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Reinitializes {@link FileSystemContext} inside {@link BaseFileSystem}.
 *
 * A daemon heartbeat thread periodically fetches configuration hashes from meta master,
 * if they differ from the hashes in the {@link alluxio.ClientContext} backing the
 * {@link FileSystemContext}, it tries to reinitialize the {@link FileSystemContext}.
 *
 * Each RPC needs to call {@link #block()} and {@link #unblock()} to mark its lifetime, when there
 * are ongoing RPCs executing between these two methods, reinitialization is blocked.
 *
 * Reinitialization starts when there are no ongoing RPCs, after starting, all further RPCs
 * are blocked until the reinitialization finishes. If it succeeds, future RPCs will use the
 * reinitialized context, otherwise, an exception is thrown from {@link #block()}.
 */
public final class FileSystemContextReinitializer implements Closeable {
  private volatile FileSystemContext mContext;
  private volatile ConfigHashSync mExecutor;
  private final ExecutorService mExecutorService;

  /**
   * Synchronize between reinitialization and RPC calls using this context.
   * RPC calls acquire read lock during their lifetimes.
   * Reinitialization acquires write lock.
   * It's in non-fair mode, which means if there is ongoing RPC calls, the reinitialization will
   * never acquire the write lock, so reinitialization will be blocked until timeout.
   */
  private final ReadWriteLock mLock = new ReentrantReadWriteLock();

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
    mExecutorService = Executors.newFixedThreadPool(1, ThreadFactoryUtils.build(
        "config-hash-master-heartbeat-%d", true));
    mExecutorService.submit(new HeartbeatThread(HeartbeatContext.META_MASTER_CONFIG_HASH_SYNC,
        mContext.getId(), mExecutor, (int) mContext.getClientContext().getClusterConf().getMs(
        PropertyKey.USER_CONF_HASH_SYNC_INTERVAL), mContext.getClientContext().getClusterConf()));
  }

  /**
   * Resets internal states related to context without restarting the heartbeat.
   *
   * @param context the new context
   */
  public void reset(FileSystemContext context) {
    mContext = context;
    mExecutor.reset(mContext);
  }

  /**
   * Blocks reinitialization.
   *
   * When there is code running between {@link #block()} and {@link #unblock()}, reinitialization
   * will be blocked.
   *
   * When the context is being reinitialized, this call blocks until the reinitialization succeeds
   * or fails. If it fails, an exception is thrown and {@link #unblock()} is automatically called.
   *
   * If there is existing reinitialization exception, immediately throw it without trying to
   * block further reinitialization.
   */
  public void block() throws IOException {
    Optional<IOException> exception = mExecutor.getException();
    if (exception.isPresent()) {
      throw exception.get();
    }
    mLock.readLock().lock();
    exception = mExecutor.getException();
    if (exception.isPresent()) {
      unblock();
      throw exception.get();
    }
  }

  /**
   * Unblocks reinitialization.
   *
   * Should be paired with {@link #block()}, needs to be called no matter whether the RPC
   * succeeds or fails, otherwise, the reinitialization will always be blocked.
   */
  public void unblock() {
    mLock.readLock().unlock();
  }

  /**
   * Shuts down the heartbeat thread immediately.
   *
   * If already closed, this is a noop.
   */
  public void close() {
    if (!mExecutorService.isShutdown()) {
      mExecutorService.shutdownNow();
    }
  }

  /**
   * Begins reinitialization.
   *
   * Blocks until no ongoing RPCs are in the middle of {@link #block()} and {@link #unblock()},
   * or timeouts.
   * When it returns without timing out, further RPCs calling {@link #block()} will be blocked
   * until {@link #end()}.
   *
   * The timeout is specified as {@link PropertyKey#USER_CONF_HASH_SYNC_TIMEOUT}.
   *
   * @throws TimeoutException if timed out
   * @throws InterruptedException if the current thread is interrupted while being blocked
   */
  public void begin() throws TimeoutException, InterruptedException {
    long timeout = mContext.getClusterConf().getMs(PropertyKey.USER_CONF_HASH_SYNC_TIMEOUT);
    if (!mLock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Failed to begin reinitialization after being blocked for "
          + DurationFormatUtils.formatDurationWords(timeout, true, true));
    }
  }

  /**
   * Ends reinitialization.
   *
   * RPCs blocking on {@link #block()} should be unblocked.
   */
  public void end() {
    mLock.writeLock().unlock();
  }
}
