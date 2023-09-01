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
import alluxio.master.MasterClientContext;

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
public class ConfigHashSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigHashSync.class);

  private final FileSystemContext mContext;

  private volatile IOException mException;

  /**
   * Constructs a new {@link ConfigHashSync}.
   *
   * @param context the filesystem context
   */
  public ConfigHashSync(FileSystemContext context) {
    mContext = context;
  }

  /**
   * Resets the internal meta master client based on the new configuration.
   *
   * @param context the context containing the new configuration
   */
  public void resetMetaMasterConfigClient(MasterClientContext context) {
  }

  protected RetryHandlingMetaMasterConfigClient createMetaMasterConfigClient(
      MasterClientContext context) {
    return new RetryHandlingMetaMasterConfigClient(context);
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
  public synchronized void heartbeat(long timeLimitMs) {
    // No op so far
    // will add according to the real requirement
  }

  @Override
  public void close() {
  }
}
