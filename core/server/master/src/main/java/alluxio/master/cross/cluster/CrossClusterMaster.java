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

package alluxio.master.cross.cluster;

import alluxio.exception.status.UnavailableException;
import alluxio.master.Master;
import alluxio.proto.journal.CrossCluster.MountList;

import io.grpc.stub.StreamObserver;

/**
 * The interface for the cross cluster configuration service.
 */
public interface CrossClusterMaster extends Master {
  /**
   * Called by the cluster with cluster id to get updated mount lists of other clusters.
   * @param clusterId the subscribing cluster id
   * @param stream the response stream
   */
  void subscribeMounts(String clusterId, StreamObserver<MountList> stream);

  /**
   * Set the mount list for a cluster.
   * @param mountList the mount list
   */
  void setMountList(MountList mountList) throws UnavailableException;
}
