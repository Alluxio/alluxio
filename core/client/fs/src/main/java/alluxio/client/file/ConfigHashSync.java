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
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.master.MasterClientContext;
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

  private final FileSystemContext mContext;
  private volatile RetryHandlingMetaMasterConfigClient mClient;

  private volatile IOException mException;

  /**
   * Constructs a new {@link ConfigHashSync}.
   *
   * @param context the filesystem context
   */
  public ConfigHashSync(FileSystemContext context) {
    mContext = context;
    mClient = new RetryHandlingMetaMasterConfigClient(mContext.getMasterClientContext());
  }

  /**
   * Resets the internal meta master client based on the new configuration.
   *
   * @param context the context containing the new configuration
   */
  public void resetMetaMasterConfigClient(MasterClientContext context) {
    mClient.close();
    mClient = new RetryHandlingMetaMasterConfigClient(context);
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
  public synchronized void heartbeat() {
    if (!mContext.getClientContext().getClusterConf().clusterDefaultsLoaded()) {
      // Wait until the initial cluster defaults are loaded.
      return;
    }
    ConfigHash hash;
    try {
      hash = mClient.getConfigHash();
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to meta master to get configuration hash:", e);
      // Disconnect to reconnect in the next heartbeat.
      mClient.disconnect();
      return;
    }
    boolean isClusterConfUpdated = !hash.getClusterConfigHash().equals(
        mContext.getClientContext().getClusterConfHash());
    boolean isPathConfUpdated = !hash.getPathConfigHash().equals(
        mContext.getClientContext().getPathConfHash());
    if (isClusterConfUpdated || isPathConfUpdated) {
      try {
        mContext.reinit(isClusterConfUpdated, isPathConfUpdated);
        mException = null;
      } catch (UnavailableException e) {
        LOG.error("Failed to reinitialize FileSystemContext:", e);
        // Meta master might be temporarily unavailable, retry in next heartbeat.
      } catch (IOException e) {
        LOG.error("Failed to close FileSystemContext, interrupting the heartbeat thread", e);
        mException = e;
        // If the heartbeat keeps running, the context might be reinitialized successfully in the
        // next heartbeat, then the resources that are not closed in the old context are leaked.
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void close() {
    mClient.close();
  }
}
