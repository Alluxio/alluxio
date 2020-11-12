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

import alluxio.util.webui.UIMountPointInfo;

import com.google.common.base.MoreObjects;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Serializable;
import java.util.Map;

/**
 * Alluxio WebUI mount table information.
 */
@NotThreadSafe
public final class MasterWebUIMountTable implements Serializable {
  private static final long serialVersionUID = -2903043308252679410L;

  private boolean mDebug;
  private UIMountPointInfo[] mMountPointInfos;

  /**
   * Creates a new instance of {@link MasterWebUIMountTable}.
   */
  public MasterWebUIMountTable() {
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
   * Get mount point infos [ ].
   *
   * @return the mount point info [ ]
   */
  public UIMountPointInfo[] getMountPointInfos() {
    return mMountPointInfos;
  }

  /**
   * Sets debug.
   *
   * @param Debug the debug
   * @return the debug
   */
  public MasterWebUIMountTable setDebug(boolean Debug) {
    mDebug = Debug;
    return this;
  }

  /**
   * Sets mount point infos.
   *
   * @param mountPoints the mount points
   * @return the mount point infos
   */
  public MasterWebUIMountTable setMountPointInfos(
      Map<String, MountPointInfo> mountPoints) {
    UIMountPointInfo[] mountPointInfos = new UIMountPointInfo[mountPoints.size()];
    int i = 0;
    for (Map.Entry<String, MountPointInfo> entry : mountPoints.entrySet()) {
      mountPointInfos[i] = new UIMountPointInfo(entry.getKey(), entry.getValue());
      i++;
    }
    mMountPointInfos = mountPointInfos;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("mountPointInfos", mMountPointInfos)
        .toString();
  }
}
