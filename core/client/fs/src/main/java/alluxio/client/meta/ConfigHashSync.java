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

package alluxio.client.meta;

import alluxio.client.file.FileSystemContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.wire.ConfigHash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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

  private final RetryHandlingMetaMasterConfigClient mClient;
  private final FileSystemContext mContext;
  private ConfigHash mHash;

  /**
   * Constructs a new {@link ConfigHashSync}.
   *
   * @param client the meta master client
   * @param context the filesystem context
   */
  public ConfigHashSync(RetryHandlingMetaMasterConfigClient client, FileSystemContext context) {
    mClient = client;
    mContext = context;
  }

  @Override
  public synchronized void heartbeat() throws InterruptedException {
    try {
      ConfigHash version = mClient.getConfigHash();
      if (!mHash.equals(version)) {
        // This may take long time and block the heartbeat, but it's fine since it's meaningless
        // to run the heartbeat during the re-initialization.
        mContext.reinit();
        mHash = version;
      }
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to meta master to get configuration version:", e);
      mClient.disconnect();
    }
  }

  @Override
  public synchronized void close() {
    // Noop, the mClient will be closed by users of this class.
  }
}
