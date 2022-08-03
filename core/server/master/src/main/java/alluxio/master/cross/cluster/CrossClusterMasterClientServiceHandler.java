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

import alluxio.RpcUtils;
import alluxio.grpc.ClusterId;
import alluxio.grpc.CrossClusterMasterClientServiceGrpc;
import alluxio.grpc.MountList;
import alluxio.grpc.SetMountListResponse;

import com.google.common.base.Preconditions;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio client.
 */
public class CrossClusterMasterClientServiceHandler
    extends CrossClusterMasterClientServiceGrpc.CrossClusterMasterClientServiceImplBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(CrossClusterMasterClientServiceHandler.class);
  private final CrossClusterMaster mCrossClusterMaster;

  /**
   * @param crossClusterMaster the cross cluster master object
   */
  public CrossClusterMasterClientServiceHandler(CrossClusterMaster crossClusterMaster) {
    mCrossClusterMaster = Preconditions.checkNotNull(crossClusterMaster);
  }

  @Override
  public void subscribeMounts(ClusterId clusterId, StreamObserver<MountList> stream) {
    try {
      RpcUtils.callAndReturn(LOG, () -> {
        mCrossClusterMaster.subscribeMounts(clusterId.getClusterId(), stream);
        return null;
      }, "SubscribeMounts", false, "request=%s", clusterId);
    } catch (Exception e) {
      stream.onError(e);
    }
  }

  @Override
  public void setMountList(MountList mountList, StreamObserver<SetMountListResponse> stream) {
    RpcUtils.call(LOG, () -> {
      mCrossClusterMaster.setMountList(mountList);
      return SetMountListResponse.getDefaultInstance();
    }, "SetMountList", "request=%s", stream);
  }
}
