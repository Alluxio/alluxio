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

package alluxio.fuse.meta;

import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.check.UpdateCheck;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.util.network.NetworkAddressUtils;

import jdk.internal.joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Periodically Alluxio version update check.
 */
@NotThreadSafe
public final class UpdateChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(UpdateChecker.class);

  private String mInstanceId;
  private List<String> mUnchangeableFuseInfo;

  /**
   * Creates a new instance of {@link UpdateChecker}.
   *
   */
  public UpdateChecker() {}

  /**
   * Heartbeat for the periodic update check.
   */
  @Override
  public void heartbeat() {
    if (mInstanceId == null) {
      mInstanceId = getNewInstanceId();
    }
    if (mUnchangeableFuseInfo == null) {
      mUnchangeableFuseInfo = getUnchangeableFuseInfo();
    }
    try {
      String latestVersion =
          UpdateCheck.getLatestVersion(mInstanceId, mUnchangeableFuseInfo,
              3000, 3000, 3000);
      if (!ProjectConstants.VERSION.equals(latestVersion)) {
        LOG.info("The latest version (" + latestVersion + ") is not the same "
            + "as the current version (" + ProjectConstants.VERSION + "). To upgrade "
            + "visit https://www.alluxio.io/download/.");
      }
    } catch (Throwable t) {
      LOG.debug("Unable to check for updates:", t);
    }
  }

  @Override
  public void close() {}

  private List<String> getUnchangeableFuseInfo() {
    List<String> fuseInfo = new ArrayList<>();
    // cache settings, kernel/user data/metadata cache enabled
    // connecting to UFS or Alluxio, which UFS
    return Collections.unmodifiableList(fuseInfo);
  }

  private List<String> getFuseInfo(List<String> unchangeableInfo) {
    List<String> fuseInfo = new ArrayList<>();
    // operations, hasWrite, hasTruncate, hasChmod.... e.g.
    fuseInfo.addAll(unchangeableInfo);
    return Collections.unmodifiableList(fuseInfo);
  }

  private String getNewInstanceId() {
    List<String> components = new ArrayList<>();
    // Avoid throwing RuntimeException when hostname
    // is not resolved on metrics reporting
    try {
      components.add(NetworkAddressUtils.getLocalHostName(5 * Constants.SECOND_MS)
          .replace(UpdateCheck.USER_AGENT_SEPARATOR, "-"));
    } catch (RuntimeException e) {
      LOG.debug("Can't find local host name", e);
    }
    try {
      components.add(NetworkAddressUtils.getLocalIpAddress(5 * Constants.SECOND_MS)
          .replace(UpdateCheck.USER_AGENT_SEPARATOR, "-"));
    } catch (RuntimeException e) {
      LOG.debug("Can't find local ip", e);
    }
    components.add(UUID.randomUUID().toString());
    return Strings.join(components, "-");
  }
}
