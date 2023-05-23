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

package alluxio.wire;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI heartbeat threads information.
 */
@NotThreadSafe
public final class WebUIHeartbeatThreads implements Serializable {
  private static final long serialVersionUID = -2903043308252679410L;

  private boolean mDebug;
  private HeartbeatThreadInfo[] mHeartbeatThreadInfos;

  /**
   * Creates a new instance of {@link WebUIHeartbeatThreads}.
   */
  public WebUIHeartbeatThreads() {
  }

  /**
   * Gets debug.
   *
   * @return the debug
   */
  public boolean getDebug() {
    return mDebug;
  }

  /**
   * Get heartbeat thread infos [ ].
   *
   * @return the heartbeat thread info [ ]
   */
  public HeartbeatThreadInfo[] getHeartbeatThreadInfos() {
    return mHeartbeatThreadInfos;
  }

  /**
   * Sets debug.
   *
   * @param Debug the debug
   * @return the debug
   */
  public WebUIHeartbeatThreads setDebug(boolean Debug) {
    mDebug = Debug;
    return this;
  }

  /**
   * Sets heartbeat thread infos.
   *
   * @param heartbeatThreadInfos the heartbeat threads info
   * @return the heartbeat thread infos
   */
  public WebUIHeartbeatThreads setHeartbeatThreadInfos(
      Map<String, HeartbeatThreadInfo> heartbeatThreadInfos) {
    mHeartbeatThreadInfos = heartbeatThreadInfos.values().toArray(new HeartbeatThreadInfo[0]);
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("heartbeatThreadInfos", mHeartbeatThreadInfos)
        .toString();
  }
}
