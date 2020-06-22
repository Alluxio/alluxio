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

package alluxio.master.meta;

import alluxio.ProjectConstants;
import alluxio.heartbeat.HeartbeatExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically Alluxio version update check.
 */
@NotThreadSafe
public final class UpdateChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateChecker.class);
  private MetaMaster mMetaMaster;

  /**
   * Creates a new instance of {@link UpdateChecker}.
   *
   * @param metaMaster the meta master
   */
  public UpdateChecker(DefaultMetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  /**
   * Heartbeat for the periodically update check.
   */
  @Override
  public void heartbeat() {
    try {
      String latestVersion =
          UpdateCheck.getLatestVersion(mMetaMaster.getClusterID(), 3000, 3000, 3000);
      if (!ProjectConstants.VERSION.equals(latestVersion)) {
        LOG.info("The latest version (" + latestVersion + ") is not the same "
            + "as the current version (" + ProjectConstants.VERSION + "). To upgrade "
            + "visit https://www.alluxio.io/download/.");
        mMetaMaster.setNewerVersionAvailable(true);
      }
    } catch (Throwable t) {
      LOG.debug("Unable to check for updates:", t);
    }
  }

  @Override
  public void close() {}
}
