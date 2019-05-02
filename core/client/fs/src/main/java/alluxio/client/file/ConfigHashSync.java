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

import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.wire.ConfigHash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Heartbeat task for getting the latest configuration versions from meta master, if versions
 * change, then re-initialize the filesystem context.
 *
 * If a heartbeat fails, the internal meta master client will disconnect and try to reconnect.
 * The passed in client will not be closed by this class.
 */
@ThreadSafe
public final class ConfigHashSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigHashSync.class);

  private volatile FileSystemContext mContext;
  private volatile RetryHandlingMetaMasterConfigClient mClient;

  private volatile IOException mException;

  /**
   * Constructs a new {@link ConfigHashSync}.
   *
   * @param context the filesystem context
   */
  public ConfigHashSync(FileSystemContext context) {
    init(context);
  }

  /**
   * Resets internal states related to context.
   *
   * @param context the new context
   */
  public void reset(FileSystemContext context) {
    init(context);
  }

  private void init(FileSystemContext context) {
    mContext = context;
    mClient = new RetryHandlingMetaMasterConfigClient(mContext.getMasterClientContext());
  }

  /**
   * @return empty if there is no exception during reinitialization, otherwise, return the exception
   */
  public Optional<IOException> getException() {
    if (mException == null) {
      return Optional.empty();
    }
    return Optional.of(mException);
  }

  @Override
  public synchronized void heartbeat() throws InterruptedException {
    try {
      if (!mContext.getClientContext().getConf().clusterDefaultsLoaded()) {
        // Wait until the initial cluster defaults are loaded.
        return;
      }
      ConfigHash hash = mClient.getConfigHash();
      boolean isClusterConfUpdated = !hash.getClusterConfigHash().equals(
          mContext.getClientContext().getClusterConfHash());
      boolean isPathConfUpdated = !hash.getPathConfigHash().equals(
          mContext.getClientContext().getPathConfHash());
      if (isClusterConfUpdated || isPathConfUpdated) {
        try {
          mContext.reinit();
          mException = null;
        } catch (IOException e) {
          mException = e;
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to meta master to get configuration hash:", e);
      mClient.disconnect();
    }
  }

  @Override
  public synchronized void close() {
    // Noop, the mClient will be closed by users of this class.
  }
}
