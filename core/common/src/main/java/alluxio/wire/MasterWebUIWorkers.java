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

import alluxio.util.webui.NodeInfo;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI workers information.
 */
@NotThreadSafe
public final class MasterWebUIWorkers implements Serializable {
  private static final long serialVersionUID = 1320235517686567983L;

  private boolean mDebug;
  private NodeInfo[] mFailedNodeInfos;
  private NodeInfo[] mNormalNodeInfos;

  /**
   * Creates a new instance of {@link MasterWebUIWorkers}.
   */
  public MasterWebUIWorkers() {
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
   * Get failed node infos node info [ ].
   *
   * @return the node info [ ]
   */
  public NodeInfo[] getFailedNodeInfos() {
    return mFailedNodeInfos;
  }

  /**
   * Get normal node infos node info [ ].
   *
   * @return the node info [ ]
   */
  public NodeInfo[] getNormalNodeInfos() {
    return mNormalNodeInfos;
  }

  /**
   * Sets debug.
   *
   * @param Debug the debug
   * @return the debug
   */
  public MasterWebUIWorkers setDebug(boolean Debug) {
    mDebug = Debug;
    return this;
  }

  /**
   * Sets failed node infos.
   *
   * @param FailedNodeInfos the failed node infos
   * @return the failed node infos
   */
  public MasterWebUIWorkers setFailedNodeInfos(NodeInfo[] FailedNodeInfos) {
    mFailedNodeInfos = FailedNodeInfos.clone();
    return this;
  }

  /**
   * Sets normal node infos.
   *
   * @param NormalNodeInfos the normal node infos
   * @return the normal node infos
   */
  public MasterWebUIWorkers setNormalNodeInfos(NodeInfo[] NormalNodeInfos) {
    mNormalNodeInfos = NormalNodeInfos.clone();
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("debug", mDebug)
        .add("failedNodeInfos", mFailedNodeInfos).add("normalNodeInfos", mNormalNodeInfos)
        .toString();
  }
}
