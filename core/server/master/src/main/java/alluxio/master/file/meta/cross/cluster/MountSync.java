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

package alluxio.master.file.meta.cross.cluster;

import alluxio.grpc.PathSubscription;
import alluxio.grpc.UfsInfo;
import alluxio.master.file.meta.options.MountInfo;

import com.google.common.annotations.VisibleForTesting;

import java.util.Objects;

/**
 * Basic mount information.
 */
public class MountSync {
  private final String mClusterId;
  private final String mUfsPath;

  @Override
  public String toString() {
    return "{ ClusterId: " + mClusterId + ", UfsPath: " + mUfsPath + " }";
  }

  /**
   * Create a new mount sync from a path subscription.
   * @param path the path subscription
   * @return the new mount sync
   */
  public static MountSync fromPathSubscription(PathSubscription path) {
    return new MountSync(path.getClusterId(), path.getUfsPath());
  }

  /**
   * Create a new mount sync from a MountInfo object.
   * @param clusterId the cluster id
   * @param info the mount info
   * @return the new mount sync
   */
  public static MountSync fromMountInfo(String clusterId, MountInfo info) {
    return new MountSync(clusterId,
        info.getUfsUri().toString());
  }

  /**
   * Create a new mount sync from a UfsInfo object.
   * @param clusterId the cluster id
   * @param info the ufs info
   * @return the new mount sync
   */
  public static MountSync fromUfsInfo(String clusterId, UfsInfo info) {
    return new MountSync(clusterId, info.getUri());
  }

  /**
   * Create a new mount sync object.
   *
   * @param clusterId the cluster id
   * @param ufsPath   the ufs path
   */
  @VisibleForTesting
  public MountSync(String clusterId, String ufsPath) {
    mClusterId = clusterId;
    mUfsPath = ufsPath;
  }

  /**
   * @return the cluster id
   */
  public String getClusterId() {
    return mClusterId;
  }

  /**
   * @return the ufs path
   */
  public String getUfsPath() {
    return mUfsPath;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MountSync) {
      MountSync other = (MountSync) o;
      return mClusterId.equals(other.mClusterId) && mUfsPath.equals(other.mUfsPath);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mClusterId, mUfsPath);
  }
}
