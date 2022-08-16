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

package alluxio.client.cross.cluster;

import alluxio.Client;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.journal.CrossCluster.MountList;

import io.grpc.stub.StreamObserver;

/**
 * Interface for a cross cluster configuration client.
 */
public interface CrossClusterClient extends Client {

  /**
   * Subscribe to the cross cluster configuration service for a stream
   * of mount changes for external clusters.
   * @param clusterId the local cluster id
   * @param stream the stream that will process the responses
   */
  void subscribeMounts(String clusterId, StreamObserver<MountList> stream)
      throws AlluxioStatusException;

  /**
   * Set the mount list for this cluster at the configuration service.
   * @param mountList the local cluster mount list
   */
  void setMountList(MountList mountList) throws AlluxioStatusException;
}
