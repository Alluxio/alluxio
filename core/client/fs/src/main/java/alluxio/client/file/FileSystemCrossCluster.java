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

package alluxio.client.file;

import static alluxio.client.file.FileSystem.Factory.checkSortConf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.PathInvalidation;

import io.grpc.stub.StreamObserver;

import java.io.IOException;

/**
 * FileSystem interface with subscriptions to cross cluster invalidations enabled.
 */
public interface FileSystemCrossCluster extends FileSystem {

  /**
   * Factory for creating cross cluster file system client objects.
   */
  class Factory {
    /**
     * @param context the FileSystemContext to use with the FileSystem
     * @return a new FileSystem instance
     */
    public static FileSystemCrossCluster create(FileSystemContext context) {
      AlluxioConfiguration conf = context.getClusterConf();
      checkSortConf(conf);
      return new CrossClusterBaseFileSystem(context);
    }
  }

  /**
   * Subscribe for cross cluster invalidations.
   * @param localClusterId the local cluster id
   * @param ufsPath the ufs path to subscribe to
   * @param stream the stream where the returned results will be put
   */
  void subscribeInvalidations(String localClusterId, String ufsPath,
                              StreamObserver<PathInvalidation> stream)
      throws IOException, AlluxioException;
}
