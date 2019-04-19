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
import alluxio.collections.Pair;
import alluxio.heartbeat.HeartbeatExecutor;

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
public final class ConfigVersionSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigVersionSync.class);

  private final RetryHandlingMetaMasterConfigClient mClient;
  private final FileSystemContext mContext;
  private String mClusterConfVersion;
  private String mPathConfVersion;

  /**
   * Constructs a new {@link ConfigVersionSync}.
   *
   * @param client the meta master client
   * @param context the filesystem context
   */
  public ConfigVersionSync(RetryHandlingMetaMasterConfigClient client, FileSystemContext context) {
    mClient = client;
    mContext = context;
  }

  @Override
  public synchronized void heartbeat() throws InterruptedException {
    try {
      Pair<String, String> versions = mClient.getConfigurationVersion();
      if (!mClusterConfVersion.equals(versions.getFirst())
          || !mPathConfVersion.equals(versions.getSecond())) {
        // This may take long time and block the heartbeat, but it's fine since it's meaningless
        // to run the heartbeat during the re-initialization.
        mContext.reinit();
      }
      mClusterConfVersion = versions.getFirst();
      mPathConfVersion = versions.getSecond();
    } catch (IOException e) {
      LOG.error("Failed to heartbeat to meta master to get configuration version:", e);
      mClient.disconnect();
    }
  }

  @Override
  public void close() {
  }
}
