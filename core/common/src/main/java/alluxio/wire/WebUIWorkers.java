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

import com.google.common.base.Objects;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI workers information.
 */
@NotThreadSafe
public final class WebUIWorkers implements Serializable {
  private boolean mDebug;
  private NodeInfo[] mFailedNodeInfos;
  private NodeInfo[] mNormalNodeInfos;

  /**
   * Creates a new instance of {@link WebUIWorkers}.
   */
  public WebUIWorkers() {
  }

  public boolean getDebug() {
    return mDebug;
  }

  public NodeInfo[] getFailedNodeInfos() {
    return mFailedNodeInfos;
  }

  public NodeInfo[] getNormalNodeInfos() {
    return mNormalNodeInfos;
  }

  public WebUIWorkers setDebug(boolean Debug) {
    mDebug = Debug;
    return this;
  }

  public WebUIWorkers setFailedNodeInfos(NodeInfo[] FailedNodeInfos) {
    mFailedNodeInfos = FailedNodeInfos;
    return this;
  }

  public WebUIWorkers setNormalNodeInfos(NodeInfo[] NormalNodeInfos) {
    mNormalNodeInfos = NormalNodeInfos;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mDebug", mDebug)
        .add("mFailedNodeInfos", mFailedNodeInfos).add("mNormalNodeInfos", mNormalNodeInfos)
        .toString();
  }
}
