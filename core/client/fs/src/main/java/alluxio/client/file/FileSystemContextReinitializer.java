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
import alluxio.resource.CountResource;
import alluxio.util.ThreadFactoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reinitializes {@link FileSystemContext} inside {@link BaseFileSystem}.
 *
 * A daemon heartbeat thread periodically fetches configuration hashes from meta master,
 * if they differ from the hashes in the {@link alluxio.ClientContext} backing the
 * {@link FileSystemContext}, it tries to reinitialize the {@link FileSystemContext}.
 *
 * Each RPC needs to call {@link #acquireBlockResource()} to mark its lifetime, when there
 * are ongoing RPCs executing between these two methods, reinitialization is blocked.
 *
 * Reinitialization starts when there are no ongoing RPCs, after starting, all further RPCs
 * are blocked until the reinitialization finishes or until timeout. If it succeeds, future RPCs
 * will use the reinitialized context, otherwise, an exception is thrown from
 * {@link #acquireBlockResource()}.
 */
public final class FileSystemContextReinitializer implements Closeable {
  private final FileSystemContext mContext;
  private final ConfigHashSync mExecutor;
  private final ExecutorService mExecutorService;

  /**
   * Count for ongoing RPCs using {@link #mContext}.
   *
   * Reinitialization can only happen when count is 0. It sets the count to -1
   * atomically when it starts, and resets the count to 0 when it finishes.
   *
   * A {@link BaseFileSystem} RPC can only start when count is >= 0. It increases the
   * count by 1 atomically when it starts, and decreases the count by 1 when it finishes.
   */
  private AtomicLong mCount = new AtomicLong(0);

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
    mExecutorService = Executors.newSingleThreadExecutor(ThreadFactoryUtils.build(
        "config-hash-master-heartbeat-%d", true));
    mExecutorService.submit(
        new HeartbeatThread(HeartbeatContext.META_MASTER_CONFIG_HASH_SYNC, mContext.getId(),
            mExecutor, (int) mContext.getClientContext().getClusterConf()
            .getMs(PropertyKey.USER_CONF_SYNC_INTERVAL),
            mContext.getClientContext().getClusterConf(),
            mContext.getClientContext().getUserState()));
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
   * Acquires the resource to block reinitialization.
   *
   * When the context is being reinitialized, this call returns an empty optional.
   * If the reinitialization fails, an exception is thrown.
   *
   * If there is an existing reinitialization exception, immediately throw an exception
   * without trying to block further reinitialization.
   *
   * @throws IOException when reinitialization fails before or during this method
   * @return the resource
   */
  public Optional<CountResource> acquireBlockResource() throws IOException {
    Optional<IOException> exception = mExecutor.getException();
    if (exception.isPresent()) {
      throw exception.get();
    }
    if (mCount.updateAndGet(c -> c >= 0 ? c + 1 : c) <= 0) {
      return Optional.empty();
    }
    exception = mExecutor.getException();
    if (exception.isPresent()) {
      mCount.decrementAndGet();
      throw exception.get();
    }
    return Optional.of(new CountResource(mCount, false));
  }

  /**
   * Acquires the resource to allow reinitialization.
   *
   * If there are ongoing operations holding the resource returned by
   * {@link #acquireBlockResource()}, this returns an empty optional immediately.
   * When it returns a non-empty resource, no further resource can be acquired from
   * {@link #acquireBlockResource()} until the returned resource is closed.
   *
   * @return the resource
   */
  public Optional<CountResource> acquireAllowResource() {
    if (mCount.compareAndSet(0, -1)) {
      return Optional.of(new CountResource(mCount, true));
    }
    return Optional.empty();
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
}
