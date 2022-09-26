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

import alluxio.grpc.FileSystemMasterClientServiceGrpc;
import alluxio.grpc.PathInvalidation;
import alluxio.grpc.PathSubscription;
import alluxio.master.file.meta.cross.cluster.CrossClusterInvalidationStream;
import alluxio.master.file.meta.cross.cluster.MountSync;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for testing cross cluster invalidation subscriptions at masters.
 */
public class TestingFileSystemMasterClientServiceHandler
    extends FileSystemMasterClientServiceGrpc.FileSystemMasterClientServiceImplBase {

  private final List<CrossClusterInvalidationStream> mStreams = new ArrayList<>();

//  private final FileSystemMaster mFileSystemMaster;
//
//  /**
//   * @param fileSystemMaster the file system master
//   */
//  public TestingFileSystemMasterClientServiceHandler(FileSystemMaster fileSystemMaster) {
//    mFileSystemMaster = Preconditions.checkNotNull(fileSystemMaster);
//  }

  @Override
  public synchronized void subscribeInvalidations(
      PathSubscription pathSubscription, StreamObserver<PathInvalidation> stream) {
    mStreams.add(new CrossClusterInvalidationStream(MountSync.fromPathSubscription(
        pathSubscription), stream));
  }

  /**
   * @return the list of created streams
   */
  public synchronized List<CrossClusterInvalidationStream> getStreams() {
    return new ArrayList<>(mStreams);
  }
}
