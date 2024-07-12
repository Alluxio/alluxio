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

package alluxio.conf;

import alluxio.Client;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.Scope;
import alluxio.heartbeat.HeartbeatExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically sync the config from config server.
 */
public class ClusterConfigSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(ClusterConfigSync.class);

  private final Scope mScope;
  private final Client mClient;

  /**
   * Constructs a new {@link ClusterConfigSync}.
   *
   * @param client the client interface which can be used to get config from server
   * @param scope the property scope
   */
  public ClusterConfigSync(Client client, Scope scope) {
    mClient = client;
    mScope = scope;
  }

  @Override
  public void heartbeat(long timeLimitMs) throws InterruptedException {
    if (Configuration.getBoolean(PropertyKey.CONF_SYNC_HEARTBEAT_ENABLED)) {
      try {
        Configuration.reLoadClusterConfIfConfigHashChanged(mClient.getConfAddress(), mScope);
      } catch (AlluxioStatusException e) {
        LOG.warn("Reload cluster config failed.", e);
      }
    }
  }

  @Override
  public void close() {
  }
}
