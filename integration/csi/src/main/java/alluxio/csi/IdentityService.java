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

package alluxio.csi;

import com.google.protobuf.BoolValue;
import csi.v1.Csi.GetPluginCapabilitiesResponse;
import csi.v1.Csi.GetPluginInfoResponse;
import csi.v1.Csi.PluginCapability;
import csi.v1.Csi.PluginCapability.Service;
import csi.v1.Csi.ProbeResponse;
import csi.v1.IdentityGrpc.IdentityImplBase;
import io.grpc.stub.StreamObserver;

import static csi.v1.Csi.PluginCapability.Service.Type.CONTROLLER_SERVICE;

/**
 * Implementation of the CSI identity service.
 */
public class IdentityService extends IdentityImplBase {

  @Override
  public void getPluginInfo(csi.v1.Csi.GetPluginInfoRequest request,
      StreamObserver<GetPluginInfoResponse> responseObserver) {
    GetPluginInfoResponse response = GetPluginInfoResponse.newBuilder()
        .setName("alluxio")
        .setVendorVersion("1.0.0")
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void getPluginCapabilities(
      csi.v1.Csi.GetPluginCapabilitiesRequest request,
      StreamObserver<GetPluginCapabilitiesResponse> responseObserver) {
    GetPluginCapabilitiesResponse response =
        GetPluginCapabilitiesResponse.newBuilder()
            .addCapabilities(PluginCapability.newBuilder().setService(
                Service.newBuilder().setType(CONTROLLER_SERVICE)))
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  @Override
  public void probe(csi.v1.Csi.ProbeRequest request,
      StreamObserver<ProbeResponse> responseObserver) {
    ProbeResponse response = ProbeResponse.newBuilder()
        .setReady(BoolValue.of(true))
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
