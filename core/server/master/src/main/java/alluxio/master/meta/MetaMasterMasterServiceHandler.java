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

package alluxio.master.meta;

import alluxio.RpcUtils;
import alluxio.grpc.GetMasterIdPRequest;
import alluxio.grpc.GetMasterIdPResponse;
import alluxio.grpc.MasterHeartbeatPRequest;
import alluxio.grpc.MasterHeartbeatPResponse;
import alluxio.grpc.MetaMasterMasterServiceGrpc;
import alluxio.grpc.NetAddress;
import alluxio.grpc.RegisterMasterPRequest;
import alluxio.grpc.RegisterMasterPResponse;
import alluxio.wire.Address;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a gRPC handler for meta master RPCs invoked by an Alluxio standby master.
 */
@NotThreadSafe
public final class MetaMasterMasterServiceHandler
    extends MetaMasterMasterServiceGrpc.MetaMasterMasterServiceImplBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterMasterServiceHandler.class);

  private final MetaMaster mMetaMaster;

  /**
   * Creates a new instance of {@link MetaMasterMasterServiceHandler}.
   *
   * @param metaMaster the Alluxio meta master
   */
  public MetaMasterMasterServiceHandler(MetaMaster metaMaster) {
    mMetaMaster = metaMaster;
  }

  @Override
  public void getMasterId(GetMasterIdPRequest request,
      StreamObserver<GetMasterIdPResponse> responseObserver) {
    NetAddress masterAddress = request.getMasterAddress();
    RpcUtils.call(LOG, (RpcUtils.RpcCallableThrowsIOException<GetMasterIdPResponse>) () -> {
      return GetMasterIdPResponse.newBuilder()
          .setMasterId(mMetaMaster.getMasterId(Address.fromProto(masterAddress))).build();
    }, "getMasterId", "request=%s", responseObserver, request);
  }

  @Override
  public void registerMaster(RegisterMasterPRequest request,
      StreamObserver<RegisterMasterPResponse> responseObserver) {
    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<RegisterMasterPResponse>) () -> {
          mMetaMaster.masterRegister(request.getMasterId(), request.getOptions());
          return RegisterMasterPResponse.getDefaultInstance();
        }, "registerMaster", "request=%s", responseObserver, request);
  }

  @Override
  public void masterHeartbeat(MasterHeartbeatPRequest request,
      StreamObserver<MasterHeartbeatPResponse> responseObserver) {
    RpcUtils.call(LOG,
        (RpcUtils.RpcCallableThrowsIOException<MasterHeartbeatPResponse>) () -> {
          return MasterHeartbeatPResponse.newBuilder()
              .setCommand(mMetaMaster.masterHeartbeat(request.getMasterId())).build();
        }, "masterHeartbeat", "request=%s", responseObserver, request);
  }
}
